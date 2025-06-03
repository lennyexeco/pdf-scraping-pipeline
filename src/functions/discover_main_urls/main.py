import base64
import json
import logging
import os
import time
import re
from datetime import datetime
from urllib.parse import urljoin, urlparse

import functions_framework
import requests
from bs4 import BeautifulSoup
from google.cloud import firestore
from google.cloud import pubsub_v1
import vertexai
from vertexai.generative_models import GenerationConfig, GenerativeModel, Part

from src.common.config import load_customer_config, load_dynamic_site_config
from src.common.utils import setup_logging, generate_url_hash 
from src.common.helpers import validate_html_content 

logger = logging.getLogger(__name__)

NEXT_STEP_EXTRACT_INITIAL_METADATA_TOPIC_NAME = "extract-initial-metadata-topic"
RETRY_TOPIC_NAME = "retry-pipeline" 
DEFAULT_USER_AGENT = 'GoogleCloudFunction-DiscoverMainURLs/1.2'


def fetch_page_content_via_requests(url, timeout=30, logger_instance=None, user_agent=DEFAULT_USER_AGENT):
    active_logger = logger_instance if logger_instance else logger
    try:
        response = requests.get(url, timeout=timeout, headers={'User-Agent': user_agent})
        response.raise_for_status()
        if 'text/html' not in response.headers.get('Content-Type', '').lower():
            active_logger.warning(f"Content-Type for {url} is not HTML: {response.headers.get('Content-Type')}")
        return validate_html_content(response.content, active_logger)
    except requests.exceptions.RequestException as e:
        active_logger.error(f"Failed to fetch URL {url} with requests: {e}")
        return None


def fetch_page_content_dynamically(url, project_config, logger_instance, interactions=None):
    active_logger = logger_instance if logger_instance else logger
    cloud_run_scraper_url = project_config.get("web_renderer_url")
    user_agent = project_config.get("dynamic_scraper_user_agent", DEFAULT_USER_AGENT)
    timeout_for_cloud_run_call = project_config.get("dynamic_fetch_cloud_run_timeout", 60)

    if not cloud_run_scraper_url:
        active_logger.warning(f"Dynamic fetching for {url}, but 'web_renderer_url' not configured. Falling back to static.")
        return fetch_page_content_via_requests(url, project_config.get("discovery_request_timeout", 30), active_logger, user_agent)

    renderer_interactions = []
    if interactions:
        for interaction in interactions:
            if interaction.get("type") == "click_all_visible" and interaction.get("selector"):
                renderer_interactions.append({"type": "click", "selector": interaction["selector"]})
    else:
        renderer_interactions = [
            {"type": "click", "selector": "button[aria-expanded='false']"},
            {"type": "click", "selector": ".accordion-button.collapsed"}
        ]

    payload = {
        "url": url, "user_agent": user_agent,
        "timeout": project_config.get("dynamic_fetch_render_timeout", 45000),
        "max_wait": 10, "expand_accordions": True, "trigger_lazy_loading": True,
        "include_metadata": True, "interactions": renderer_interactions
    }
    headers = {"Content-Type": "application/json"}

    try:
        active_logger.info(f"Calling web-renderer for {url} with payload: {json.dumps(payload, indent=2)}")
        response = requests.post(cloud_run_scraper_url, json=payload, headers=headers, timeout=timeout_for_cloud_run_call)
        response.raise_for_status()
        response_json = response.json()
        html_content = response_json.get("html")
        if not html_content:
            active_logger.error(f"No HTML in web-renderer response for {url}: {json.dumps(response_json)}")
            return None
        active_logger.info(f"Fetched dynamic content for {url}. Interactions: {response_json.get('interactions', [])}")
        return validate_html_content(html_content, active_logger)
    except requests.exceptions.RequestException as e:
        active_logger.error(f"Failed web-renderer call for {url}: {e}")
        return None
    except (json.JSONDecodeError, KeyError) as e:
        active_logger.error(f"Invalid response from web-renderer for {url}: {e}. Response: {response.text[:500]}")
        return None


def extract_heuristic_links(html_content, base_url, project_config, logger_instance=None):
    """
    Extracts links based on heuristics: content area, exclusion tags, text, and href patterns.
    """
    active_logger = logger_instance or logger
    if not html_content:
        return set()

    heuristic_config = project_config.get("heuristic_link_extraction", {})
    if not heuristic_config.get("enabled", False):
        active_logger.info("Heuristic link extraction is disabled in project_config.")
        return set()

    soup = BeautifulSoup(html_content, 'html.parser')
    discovered_urls = set()

    content_selectors = heuristic_config.get("content_selectors", ["main", "article", "div.content", "div.text-page-wrapper-wide"])
    exclusion_selectors = heuristic_config.get("exclusion_selectors", ["nav", "header", "footer", ".breadcrumb", ".pagination"])
    link_text_keywords = heuristic_config.get("link_text_keywords", ["report", "guide", ".pdf"])
    href_patterns = heuristic_config.get("href_patterns", [r"\.pdf$", r"/media/", r"/download/"])
    min_link_text_len = heuristic_config.get("min_link_text_len", 10)

    content_elements_to_search = []
    for selector in content_selectors:
        try:
            elements = soup.select(selector)
            if elements:
                content_elements_to_search.extend(elements)
                active_logger.debug(f"Heuristic search: Added {len(elements)} element(s) from selector '{selector}'")
        except Exception as e_select:
            active_logger.warning(f"Heuristic search: Error with content selector '{selector}': {e_select}")
            
    if not content_elements_to_search:
        active_logger.debug("Heuristic search: No specific content areas matched, searching entire body.")
        if soup.body:
            content_elements_to_search.append(soup.body)
        else:
            active_logger.warning("Heuristic search: No body tag found in HTML.")
            return set()
    
    search_scope_elements = list(dict.fromkeys(content_elements_to_search))
    active_logger.info(f"Heuristic search: Searching within {len(search_scope_elements)} unique content element(s).")

    all_links_in_scope = []
    for content_element in search_scope_elements:
        all_links_in_scope.extend(content_element.find_all('a', href=True))
    
    active_logger.info(f"Heuristic search: Found {len(all_links_in_scope)} total <a> tags in identified scope.")

    for link in all_links_in_scope:
        is_excluded = False
        for ex_selector in exclusion_selectors:
            if link.find_parent(css_selector=ex_selector):
                is_excluded = True
                break
        if is_excluded:
            continue

        href = link['href'].strip()
        text = link.get_text(strip=True)

        if not href or href.startswith(('#', 'javascript:', 'mailto:', 'tel:')) or href.strip() == '/':
            continue

        absolute_url = urljoin(base_url, href)
        parsed_abs_url = urlparse(absolute_url)
        if not parsed_abs_url.scheme or not parsed_abs_url.netloc:
            active_logger.debug(f"Heuristic search: Skipping invalid absolute URL '{absolute_url}' from href '{href}'.")
            continue
        
        base_domain = urlparse(base_url).netloc
        if parsed_abs_url.netloc != base_domain:
            if not (parsed_abs_url.netloc.endswith(base_domain) and ("download." in parsed_abs_url.netloc or "media." in parsed_abs_url.netloc)):
                 active_logger.debug(f"Heuristic search: Skipping external domain link: {absolute_url} (base: {base_domain})")
                 continue

        passes_filter = False
        for pattern in href_patterns:
            if re.search(pattern, href, re.IGNORECASE):
                passes_filter = True
                active_logger.debug(f"Heuristic match (href pattern '{pattern}'): {absolute_url} (Text: '{text}')")
                break
        
        if not passes_filter:
            if len(text) >= min_link_text_len:
                if any(keyword.lower() in text.lower() for keyword in link_text_keywords):
                    passes_filter = True
                    active_logger.debug(f"Heuristic match (text keyword, len>min): {absolute_url} (Text: '{text}')")
            elif any(keyword.lower() == text.lower() for keyword in link_text_keywords):
                 passes_filter = True
                 active_logger.debug(f"Heuristic match (exact text keyword, len<min): {absolute_url} (Text: '{text}')")

        if passes_filter:
            active_logger.info(f"Heuristic search: Adding valid URL: '{absolute_url}' (Text: '{text}')")
            discovered_urls.add(absolute_url)

    active_logger.info(f"Heuristic link extraction found {len(discovered_urls)} URLs from {base_url}")
    return discovered_urls


def extract_structured_data_items(html_content, base_url, logger_instance):
    """
    Extract items from structured data pages (tables, timetables).
    This version only creates items for rows that contain direct PDF or HTML links.
    """
    active_logger = logger_instance
    if not active_logger:
        # Fallback logger if none provided
        active_logger = logging.getLogger(__name__)
        if not active_logger.hasHandlers():
            logging.basicConfig(level=logging.INFO)

    soup = BeautifulSoup(html_content, 'html.parser')
    discovered_items = []
    
    page_title_element = soup.find('h1')
    page_title = page_title_element.get_text(strip=True) if page_title_element else "Structured Page"
    
    # Process all tables
    tables = soup.find_all('table')
    active_logger.info(f"Found {len(tables)} tables to process on page: {page_title}")
    
    for table_index, table in enumerate(tables):
        # Get table context from preceding heading
        table_context = ""
        # Find the closest preceding h2, h3, or h4 to the current table
        prev_heading = table.find_previous_sibling(['h2', 'h3', 'h4'])
        if prev_heading:
            table_context = prev_heading.get_text(strip=True)
        
        active_logger.info(f"Processing table {table_index + 1} (Context: '{table_context}')")
        
        # Process table rows (attempt to skip header row if possible, e.g., by looking for <th>)
        rows = table.find_all('tr')
        if not rows:
            active_logger.debug(f"No rows found in table {table_index + 1}")
            continue

        # A simple heuristic to skip header: if first row has <th>, skip it.
        first_row_cells = rows[0].find_all(['td', 'th'])
        if first_row_cells and all(cell.name == 'th' for cell in first_row_cells):
            active_logger.debug(f"Skipping potential header row in table {table_index + 1}")
            rows = rows[1:]
        
        for row_index, row in enumerate(rows):
            cells = row.find_all('td')
            if len(cells) < 1: # Allow rows with at least one cell if it contains a link
                active_logger.debug(f"Skipping row {row_index + 1} in table {table_index + 1} due to insufficient cells (found {len(cells)}).")
                continue
            
            # Extract text from all cells for context
            row_data = [cell.get_text(strip=True) for cell in cells]
            
            # Find links in this row
            pdf_links = []
            html_links = []
            
            for cell_index, cell in enumerate(cells):
                for link_index, link in enumerate(cell.find_all('a', href=True)):
                    href = link.get('href')
                    if not href: # Skip if href is empty
                        active_logger.debug(f"Row {row_index+1}, Cell {cell_index+1}, Link {link_index+1}: Empty href, skipping.")
                        continue

                    link_text = link.get_text(strip=True)
                    
                    try:
                        full_url = urljoin(base_url, href)
                    except ValueError as e_urljoin:
                        active_logger.warning(f"Row {row_index+1}, Cell {cell_index+1}, Link {link_index+1}: Could not join base_url '{base_url}' with href '{href}': {e_urljoin}. Skipping link.")
                        continue

                    # Basic validation of the formed URL
                    if not (full_url.startswith('http://') or full_url.startswith('https://')):
                        active_logger.debug(f"Row {row_index+1}, Cell {cell_index+1}, Link {link_index+1}: Invalid or relative URL after join: '{full_url}'. Base: '{base_url}', Href: '{href}'. Skipping link.")
                        continue
                        
                    if href.lower().endswith('.pdf'):
                        pdf_links.append({
                            'url': full_url,
                            'text': link_text or f"PDF Document from {table_context or page_title}"
                        })
                    elif not href.startswith(('#', 'javascript:', 'mailto:', 'tel:')):
                        # Check if it's a link to the same page fragment, if so, might not be a "mainURL"
                        if '#' in full_url and full_url.split('#')[0] == base_url.split('#')[0]:
                             active_logger.debug(f"Row {row_index+1}, Cell {cell_index+1}, Link {link_index+1}: Skipping same-page fragment link: {full_url}")
                             continue

                        html_links.append({
                            'url': full_url,
                            'text': link_text or f"HTML Document from {table_context or page_title}"
                        })
            
            # Create items for PDF links (separate documents)
            for pdf_link_info in pdf_links:
                item_title = pdf_link_info['text']
                # If link text is very generic, try to use the first cell's data as part of the title
                if (not item_title or item_title.lower() in ["pdf", "download", "view"]) and row_data and row_data[0]:
                    item_title = f"{row_data[0]} (PDF)"
                elif not item_title: # Fallback if still no title
                     item_title = f"PDF from {table_context or page_title} - Row {row_index +1}"

                item = {
                    'main_url': pdf_link_info['url'],
                    'document_type': 'PDF_DOCUMENT',
                    'title': item_title,
                    'description': ' | '.join(filter(None, row_data)), # Join non-empty cell data
                    'table_context': table_context,
                    'page_title_context': page_title,
                    'table_row_data': row_data, # Keep original row data for context
                    'extraction_method': 'table_pdf_link'
                }
                discovered_items.append(item)
                active_logger.info(f"Found PDF link: '{item_title}' -> {pdf_link_info['url']}")
            
            # Create items for HTML links (separate documents)
            for html_link_info in html_links:
                item_title = html_link_info['text']
                if (not item_title or item_title.lower() in ["link", "details", "view", "more info"]) and row_data and row_data[0]:
                    item_title = f"{row_data[0]} (HTML)"
                elif not item_title:
                    item_title = f"Document from {table_context or page_title} - Row {row_index+1}"

                item = {
                    'main_url': html_link_info['url'],
                    'document_type': 'HTML_DOCUMENT',
                    'title': item_title,
                    'description': ' | '.join(filter(None, row_data)),
                    'table_context': table_context,
                    'page_title_context': page_title,
                    'table_row_data': row_data,
                    'extraction_method': 'table_html_link'
                }
                discovered_items.append(item)
                active_logger.info(f"Found HTML link: '{item_title}' -> {html_link_info['url']}")
            
            # --- MODIFICATION ---
            # The block that created 'STRUCTURED_DATA' for rows without links has been removed.
            # If you wanted to restore that behavior, you would add it back here,
            # typically checking `if not pdf_links and not html_links and len(row_data) >= 2:`
            # and then creating an item with 'document_type': 'STRUCTURED_DATA'.
            if not pdf_links and not html_links:
                active_logger.debug(f"Row {row_index + 1} in table {table_index + 1} did not yield any distinct PDF or HTML links. No item created for this row itself.")

    active_logger.info(f"Extracted {len(discovered_items)} items with distinct URLs from tables on page '{page_title}'")
    return discovered_items


def extract_navigation_items(html_content, base_url, project_config, logger_instance):
    """Extract items from navigation/link pages using heuristics"""
    active_logger = logger_instance
    discovered_items = []
    
    # Use existing heuristic extraction
    heuristic_urls = extract_heuristic_links(html_content, base_url, project_config, logger_instance)
    
    soup = BeautifulSoup(html_content, 'html.parser')
    
    for url in heuristic_urls:
        # Determine document type
        if url.lower().endswith('.pdf'):
            doc_type = 'PDF_DOCUMENT'
        else:
            doc_type = 'HTML_DOCUMENT'
        
        # Try to find the link element to get text
        link_text = ""
        for link in soup.find_all('a', href=True):
            link_href = urljoin(base_url, link['href'])
            if link_href == url:
                link_text = link.get_text(strip=True)
                break
        
        item = {
            'main_url': url,
            'document_type': doc_type,
            'title': link_text or f"Document from {urlparse(base_url).path}",
            'description': f"Link discovered from navigation page: {link_text}",
            'extraction_method': 'heuristic_navigation'
        }
        discovered_items.append(item)
    
    active_logger.info(f"Extracted {len(discovered_items)} navigation items")
    return discovered_items


def analyze_category_page_with_ai(html_content, project_config, gcp_project_id, main_category_url, logger_instance=None):
    active_logger = logger_instance if logger_instance else logger
    ai_model_cfg = project_config.get("ai_model_config", {})
    model_id = ai_model_cfg.get("discovery_model_id", "gemini-2.0-flash-lite-001") 
    vertex_ai_location = project_config.get("vertex_ai_location", "us-central1")

    if not html_content:
        active_logger.warning("No HTML content provided to AI for analysis.")
        return None

    try:
        vertexai.init(project=gcp_project_id, location=vertex_ai_location)
        model = GenerativeModel(model_id)

        max_html_len = project_config.get("ai_discovery_max_html_len", 3000000)
        truncated_html = html_content[:max_html_len]
        if len(html_content) > max_html_len:
            active_logger.warning(f"HTML for {main_category_url} truncated from {len(html_content)} to {max_html_len} for AI.")

        prompt = f"""
        Analyze the HTML from a category/listing page: {main_category_url}
        HTML Snippet (potentially truncated):
        ```html
        {truncated_html}
        ```
        Your task is to identify:
        1.  `main_url_selectors`: CSS selectors for links to individual articles/documents/products. Prioritize robust selectors for list-like structures. Avoid navigation/ads. If the page structure does not show a clear list of items, return an empty list [].
        2.  `pagination_selector`: CSS selector for the "next page" or "load more" link (<a> tag). Use null if not apparent.
        3.  `category_page_metadata`: JSON object of metadata on this category page applicable to all items (e.g., {{"category_name": "Electronics"}}). Empty object {{}} if none.
        4.  `interaction_selectors_for_hidden_content`: (Optional) A list of CSS selectors for elements that need to be clicked to reveal more items or links (e.g., accordion triggers, "show more" buttons that don't paginate but expand current page content). Example: ["button.accordion-trigger", "span.show-more-items"]. If none, provide an empty list [].

        Return a single, valid JSON object. Example:
        {{
          "main_url_selectors": ["article.item > h2 > a", "div.product-card > a.product-link"],
          "pagination_selector": "a.pagination-next",
          "category_page_metadata": {{ "current_category": "Example Category" }},
          "interaction_selectors_for_hidden_content": ["button.accordion-title"]
        }}
        If no clear main URL selectors for list items, use empty list for "main_url_selectors". If no pagination, use null for `pagination_selector`.
        """
        generation_config_params = project_config.get("ai_discovery_generation_config", {
            "temperature": 0.2, "top_p": 0.8, "top_k": 20, "max_output_tokens": 2048
        })
        generation_config_params["response_mime_type"] = "application/json"
        generation_config_obj = GenerationConfig(**generation_config_params)

        active_logger.info(f"Sending request to Vertex AI (model: {model_id}) for category page analysis: {main_category_url}")
        response = model.generate_content([Part.from_text(prompt)], generation_config=generation_config_obj)

        if response.candidates and response.candidates[0].content.parts:
            response_text = response.candidates[0].content.parts[0].text
            active_logger.info(f"AI analysis raw response for {main_category_url}: {response_text[:500]}...")
            try:
                ai_derived_config = json.loads(response_text)
                if isinstance(ai_derived_config.get("main_url_selectors"), list) and \
                   (ai_derived_config.get("pagination_selector") is None or isinstance(ai_derived_config.get("pagination_selector"), str)) and \
                   isinstance(ai_derived_config.get("category_page_metadata"), dict) and \
                   isinstance(ai_derived_config.get("interaction_selectors_for_hidden_content", []), list):
                    active_logger.info(f"Successfully parsed AI-derived config for {main_category_url}: {ai_derived_config}")
                    return ai_derived_config
                else:
                    active_logger.error(f"AI response for {main_category_url} has incorrect structure: {response_text}")
                    return None
            except json.JSONDecodeError as e:
                active_logger.error(f"Failed to decode AI JSON response for {main_category_url}: {e}. Response: {response_text}")
                return None
        else:
            active_logger.warning(f"No valid candidates in AI response for {main_category_url}. Response: {response}")
            return None
    except Exception as e:
        active_logger.error(f"Vertex AI analysis failed for {main_category_url}: {e}", exc_info=True)
        return None


def extract_links_using_selectors(html_content, selectors, base_url, logger_instance=None):
    active_logger = logger_instance if logger_instance else logger
    if not html_content or not selectors:
        return set()
    soup = BeautifulSoup(html_content, 'html.parser')
    extracted_urls = set()
    for selector in selectors:
        if not selector or not isinstance(selector, str):
            active_logger.warning(f"Skipping invalid selector: {selector}")
            continue
        try:
            elements = soup.select(selector)
            for element in elements:
                href_val = None
                if element.name == 'a' and element.has_attr('href'):
                    href_val = element['href'].strip()
                elif element.has_attr('data-href'):
                    href_val = element['data-href'].strip()
                
                if href_val and not href_val.startswith(('#', 'javascript:', 'mailto:', 'tel:')) and href_val.strip() != '/':
                    absolute_url = urljoin(base_url, href_val)
                    parsed_abs_url = urlparse(absolute_url)
                    if parsed_abs_url.scheme and parsed_abs_url.netloc:
                        extracted_urls.add(absolute_url)
                    else:
                        active_logger.debug(f"Skipping invalid absolute URL '{absolute_url}' from href '{href_val}' (selector '{selector}').")
        except Exception as e: 
            active_logger.warning(f"Error applying selector '{selector}' on base {base_url}: {e}")
    return extracted_urls


@functions_framework.cloud_event
def discover_main_urls(cloud_event):
    global logger 
    pubsub_message_data = base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8")
    message_payload = json.loads(pubsub_message_data)
    message_id = cloud_event.data["message"].get("messageId", "N/A")

    customer = message_payload.get("customer")
    project_config_name = message_payload.get("project")
    initial_category_url = message_payload.get("category_url")

    use_dynamic_fetching_override = message_payload.get("use_dynamic_fetching", False)
    interaction_selectors_override = message_payload.get("interaction_selectors_for_dynamic_fetch")
    override_main_url_selectors = message_payload.get("override_main_url_selectors")
    override_pagination_selector = message_payload.get("override_pagination_selector")

    logger = setup_logging(customer, project_config_name)
    active_logger = logger 
    active_logger.info(f"Received {message_id} for discover_main_urls. Customer: {customer}, Project: {project_config_name}, URL: {initial_category_url}. Dynamic fetch override: {use_dynamic_fetching_override}")

    if not all([customer, project_config_name, initial_category_url]):
        active_logger.error("Missing required fields: customer, project, or category_url.")
        return {'status': 'error', 'message': 'Missing required fields.'} 

    publisher_client = None 
    try:
        customer_config = load_customer_config(customer)
        gcp_project_id = customer_config.get("gcp_project_id", os.environ.get("GCP_PROJECT"))
        if not gcp_project_id:
            active_logger.error("GCP Project ID not configured.")
            return {'status': 'error', 'message': 'GCP Project ID not configured.'}

        db = firestore.Client(project=gcp_project_id, database=customer_config.get("firestore_database_id", "(default)"))
        project_config = load_dynamic_site_config(db, project_config_name, active_logger)
        
        publisher_client = pubsub_v1.PublisherClient()
        output_topic_path = publisher_client.topic_path(gcp_project_id, NEXT_STEP_EXTRACT_INITIAL_METADATA_TOPIC_NAME)
        retry_topic_path = publisher_client.topic_path(gcp_project_id, RETRY_TOPIC_NAME)

        max_pagination_depth = project_config.get("max_pagination_depth", 5)
        request_timeout = project_config.get("discovery_request_timeout", 30)
        all_discovered_main_urls = set()
        processed_page_urls = set()
        current_page_url = initial_category_url
        current_depth = 0
        first_page_ai_derived_config = None
        
        heuristic_config = project_config.get("heuristic_link_extraction", {})
        heuristic_enabled = heuristic_config.get("enabled", False)
        heuristic_threshold = heuristic_config.get("discovery_threshold", 1)

        default_discovery_fetch_dynamic = project_config.get("discovery_fetch_dynamic_by_default", False)
        use_dynamic_fetching = use_dynamic_fetching_override or default_discovery_fetch_dynamic
        interaction_selectors_for_dynamic_page = interaction_selectors_override or []

        while current_page_url and current_depth < max_pagination_depth and current_page_url not in processed_page_urls:
            active_logger.info(f"Processing page (Depth {current_depth + 1}): {current_page_url}. Dynamic fetch: {use_dynamic_fetching}")
            processed_page_urls.add(current_page_url)
            html_content = None
            newly_discovered_items_on_page = []

            # --- Fetching logic (dynamic/static) ---
            if use_dynamic_fetching:
                interactions_for_current_page = interaction_selectors_for_dynamic_page
                if current_depth == 0 and first_page_ai_derived_config and first_page_ai_derived_config.get("interaction_selectors_for_hidden_content"):
                    interactions_from_ai = first_page_ai_derived_config.get("interaction_selectors_for_hidden_content", [])
                    if interactions_from_ai:
                        interactions_for_current_page = [{'type': 'click_all_visible', 'selector': sel} for sel in interactions_from_ai]
                        active_logger.info(f"Using AI-suggested interactions for dynamic fetch: {interactions_for_current_page}")
                html_content = fetch_page_content_dynamically(current_page_url, project_config, active_logger, interactions=interactions_for_current_page)
            
            if not html_content:
                 active_logger.info(f"Falling back/using static fetch for {current_page_url}.")
                 html_content = fetch_page_content_via_requests(current_page_url, timeout=request_timeout, logger_instance=active_logger)

            if not html_content:
                active_logger.warning(f"No HTML content fetched for {current_page_url}. Stopping pagination for this branch.")
                break

            # --- NEW: Detect page type and extract structured items ---
            soup = BeautifulSoup(html_content, 'html.parser')
            page_title = soup.find('h1').get_text(strip=True) if soup.find('h1') else ""
            
            discovered_items = []
            
            # Check if this is a structured data page (timetables, tables)
            tables = soup.find_all('table')
            is_structured_page = (len(tables) >= 2 or 'timetable' in page_title.lower() or 
                                 'developments' in page_title.lower() or 'tracker' in page_title.lower())
            
            if is_structured_page:
                active_logger.info(f"Processing as structured data page: {current_page_url}")
                discovered_items = extract_structured_data_items(html_content, current_page_url, active_logger)
            else:
                active_logger.info(f"Processing as navigation/link page: {current_page_url}")
                discovered_items = extract_navigation_items(html_content, current_page_url, project_config, active_logger)
                
                # If heuristics found insufficient items and we have CSS/AI fallback, try those too
                proceed_with_css_ai = not heuristic_enabled or len(discovered_items) < heuristic_threshold
                
                if proceed_with_css_ai:
                    active_logger.info(f"Heuristic items ({len(discovered_items)}) below threshold ({heuristic_threshold}) or disabled. Trying CSS/AI selectors for {current_page_url}.")
                    
                    main_url_selectors_to_use = []
                    pagination_selector_to_use = None
                    category_page_metadata = {}
                    
                    if override_main_url_selectors:
                        main_url_selectors_to_use = override_main_url_selectors
                        active_logger.info(f"Using overridden main_url_selectors: {main_url_selectors_to_use}")
                    if override_pagination_selector:
                        pagination_selector_to_use = override_pagination_selector
                        active_logger.info(f"Using overridden pagination_selector: {pagination_selector_to_use}")

                    # A. Site-wide selectors (if not overridden)
                    if not main_url_selectors_to_use and project_config.get("discovery_selectors") and project_config.get("discovery_selectors").get("main_url_selectors"):
                        site_wide_cfg = project_config.get("discovery_selectors", {})
                        main_url_selectors_to_use = site_wide_cfg.get("main_url_selectors", [])
                        if not pagination_selector_to_use: pagination_selector_to_use = site_wide_cfg.get("pagination_selector")
                        active_logger.info(f"Using site-wide selectors for {current_page_url}: {main_url_selectors_to_use}")
                    
                    # B. Static fallback (if site-wide not found/empty and not overridden)
                    if not main_url_selectors_to_use:
                        fallback_cfg = project_config.get("discovery_fallback_selectors", {})
                        main_url_selectors_to_use = fallback_cfg.get("main_url_selectors", [])
                        if not pagination_selector_to_use: pagination_selector_to_use = fallback_cfg.get("pagination_selector")
                        active_logger.warning(f"Using static fallback selectors for {current_page_url}: {main_url_selectors_to_use}")
                    
                    # C. AI (last resort for selectors, or if pagination_selector still needed) - only on first page unless overridden
                    if current_depth == 0 and not (override_main_url_selectors and override_pagination_selector):
                         # Run AI if we still don't have main_url_selectors OR we don't have a pagination_selector
                        if not main_url_selectors_to_use or not pagination_selector_to_use:
                            active_logger.info(f"Attempting live AI analysis for selectors for page {current_page_url}.")
                            ai_analysis_result = analyze_category_page_with_ai(html_content, project_config, gcp_project_id, current_page_url, active_logger)
                            if ai_analysis_result:
                                first_page_ai_derived_config = ai_analysis_result # Store for reuse
                                if not main_url_selectors_to_use: # Prioritize other selectors if they exist
                                    main_url_selectors_to_use = ai_analysis_result.get("main_url_selectors", [])
                                if not pagination_selector_to_use:
                                    pagination_selector_to_use = ai_analysis_result.get("pagination_selector")
                                
                                category_page_metadata = ai_analysis_result.get("category_page_metadata", {}) # Always take this if AI runs
                                # Handle interactions (as before)
                                if not interaction_selectors_for_dynamic_page and ai_analysis_result.get("interaction_selectors_for_hidden_content"):
                                    interaction_selectors_for_dynamic_page = ai_analysis_result.get("interaction_selectors_for_hidden_content")
                                    if not use_dynamic_fetching:
                                        active_logger.info(f"AI suggested interactions ({interaction_selectors_for_dynamic_page}), consider enabling dynamic fetching.")
                                if main_url_selectors_to_use or pagination_selector_to_use:
                                    active_logger.info(f"AI analysis provided/updated selectors for {current_page_url}.")
                    elif not main_url_selectors_to_use and first_page_ai_derived_config: # Not first page, try reusing from first page AI
                        active_logger.info(f"Reusing AI selectors from first page for {current_page_url}.")
                        main_url_selectors_to_use = first_page_ai_derived_config.get("main_url_selectors", [])
                        if not pagination_selector_to_use: pagination_selector_to_use = first_page_ai_derived_config.get("pagination_selector")
                        category_page_metadata = first_page_ai_derived_config.get("category_page_metadata",{})

                    # --- Extract MainURLs using CSS Selectors ---
                    if main_url_selectors_to_use:
                        page_main_urls_css = extract_links_using_selectors(html_content, main_url_selectors_to_use, current_page_url, active_logger)
                        
                        # Convert CSS selector results to items format
                        for url in page_main_urls_css:
                            # Determine document type
                            if url.lower().endswith('.pdf'):
                                doc_type = 'PDF_DOCUMENT'
                            else:
                                doc_type = 'HTML_DOCUMENT'
                            
                            # Try to find link text
                            link_text = ""
                            for link in soup.find_all('a', href=True):
                                if urljoin(current_page_url, link['href']) == url:
                                    link_text = link.get_text(strip=True)
                                    break
                            
                            css_item = {
                                'main_url': url,
                                'document_type': doc_type,
                                'title': link_text or f"Document from CSS selector",
                                'description': f"Link found via CSS selector from {page_title}",
                                'extraction_method': 'css_selector'
                            }
                            discovered_items.append(css_item)
                        
                        active_logger.info(f"CSS selectors found {len(page_main_urls_css)} additional URLs on {current_page_url}.")
                    elif proceed_with_css_ai: # Only log error if we intended to use CSS/AI and found no selectors
                         active_logger.warning(f"No CSS/AI main URL selectors were determined or effective for {current_page_url}.")

            # --- NEW: Publish items with source tracking and document types ---
            newly_discovered_urls_on_page = set()
            
            for item in discovered_items:
                # Skip if we've already discovered this URL
                if item['main_url'] in all_discovered_main_urls:
                    continue
                
                # NEW: Enhanced payload with source tracking and document type
                payload_for_next = {
                    "customer": customer,
                    "project": project_config_name,
                    "main_url": item['main_url'],
                    
                    # NEW: Source tracking fields
                    "source_category_url": initial_category_url,
                    "source_page_url": current_page_url,
                    "document_type": item['document_type'],
                    
                    # NEW: Enhanced metadata
                    "document_metadata": {
                        "title": item.get('title', ''),
                        "description": item.get('description', ''),
                        "page_context": page_title,
                        "extraction_method": item.get('extraction_method', 'unknown'),
                        "table_context": item.get('table_context', ''),
                        "table_row_data": item.get('table_row_data', [])
                    },
                    "category_page_metadata": first_page_ai_derived_config.get("category_page_metadata", {}) if first_page_ai_derived_config else {}
                }
                
                try:
                    publisher_client.publish(output_topic_path, json.dumps(payload_for_next).encode("utf-8"))
                    active_logger.debug(f"Published {item['document_type']}: {item.get('title', item['main_url'])}")
                    newly_discovered_urls_on_page.add(item['main_url'])
                except Exception as e_pub:
                    active_logger.error(f"Failed to publish item {item.get('title', item['main_url'])}: {e_pub}")
            
            all_discovered_main_urls.update(newly_discovered_urls_on_page)
            
            if newly_discovered_urls_on_page:
                active_logger.info(f"Total {len(newly_discovered_urls_on_page)} new unique items published from {current_page_url}.")
            else:
                active_logger.info(f"No new items discovered on {current_page_url} after all methods.")

            # --- Handle Pagination ---
            next_page_url = None
            pagination_selector_to_use = None
            
            # Get pagination selector from various sources
            if override_pagination_selector:
                pagination_selector_to_use = override_pagination_selector
            elif first_page_ai_derived_config and first_page_ai_derived_config.get("pagination_selector"):
                pagination_selector_to_use = first_page_ai_derived_config.get("pagination_selector")
            elif project_config.get("discovery_selectors", {}).get("pagination_selector"):
                pagination_selector_to_use = project_config.get("discovery_selectors", {}).get("pagination_selector")
            elif project_config.get("discovery_fallback_selectors", {}).get("pagination_selector"):
                pagination_selector_to_use = project_config.get("discovery_fallback_selectors", {}).get("pagination_selector")
            
            if pagination_selector_to_use:
                try:
                    pag_elem = soup.select_one(pagination_selector_to_use)
                    if pag_elem and pag_elem.name == 'a' and pag_elem.has_attr('href'):
                        href = pag_elem['href'].strip()
                        if href and not href.startswith(('#', 'javascript:', 'mailto:', 'tel:')) and href.strip() != '/':
                            next_page_url = urljoin(current_page_url, href)
                            active_logger.info(f"Next page link: {next_page_url} using selector '{pagination_selector_to_use}'")
                        else: 
                            active_logger.info(f"Pagination link by '{pagination_selector_to_use}' has invalid href: '{href}'")
                    else: 
                        active_logger.info(f"Pagination selector '{pagination_selector_to_use}' not a valid <a> tag with href.")
                except Exception as e_select: 
                    active_logger.warning(f"Error with pagination selector '{pagination_selector_to_use}': {e_select}")
            else: 
                active_logger.info(f"No pagination selector for {current_page_url}.")

            current_page_url = next_page_url
            current_depth += 1
            if current_page_url: 
                time.sleep(project_config.get("discovery_pagination_delay_ms", 1000) / 1000.0)

        # --- After loop ---
        if not all_discovered_main_urls and processed_page_urls:
            active_logger.warning(f"No main URLs for {initial_category_url} after processing {len(processed_page_urls)} page(s).")
            if project_config.get("retry_on_no_main_urls_found", False):
                # (retry logic as before)
                no_data_payload = {
                    "customer": customer, "project": project_config_name,
                    "identifier": generate_url_hash(f"no_main_urls_found_{initial_category_url}"),
                    "category_url": initial_category_url,
                    "error_message": f"No main URLs discovered for category {initial_category_url}. Processed {len(processed_page_urls)} pages. Consider dynamic fetching or selector review.",
                    "stage": "discover_main_urls_no_data",
                    "retry_count": message_payload.get("retry_count", 0) + 1,
                    "item_data_snapshot": {"category_url": initial_category_url, "project_config_name": project_config_name, "last_processed_page_count": len(processed_page_urls)}
                }
                try:
                    publisher_client.publish(retry_topic_path, json.dumps(no_data_payload).encode("utf-8"))
                    active_logger.info(f"Published 'no_data_found' event to {RETRY_TOPIC_NAME} for {initial_category_url}")
                except Exception as e_pub_err:
                    active_logger.error(f"Failed to publish 'no_data_found' event for {initial_category_url}: {e_pub_err}")

        active_logger.info(f"Finished discovery for {initial_category_url}. Total unique items: {len(all_discovered_main_urls)}. Processed {len(processed_page_urls)} pages.")
        return {'status': 'success', 'category_url': initial_category_url, 'items_found_count': len(all_discovered_main_urls), "category_pages_processed": len(processed_page_urls)}

    except Exception as e:
        active_logger.error(f"Critical error in discover_main_urls for {initial_category_url}: {e}", exc_info=True)
        raise