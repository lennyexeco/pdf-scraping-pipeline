import base64
import json
import logging
import os
import time
from urllib.parse import urljoin, urlparse

import functions_framework
import requests
from bs4 import BeautifulSoup
from google.cloud import firestore
from google.cloud import pubsub_v1
import vertexai
from vertexai.generative_models import GenerationConfig, GenerativeModel, Part

# Assuming common modules are correctly structured and accessible in PYTHONPATH
from src.common.config import load_customer_config, load_dynamic_site_config
from src.common.utils import setup_logging, generate_url_hash # Added generate_url_hash
from src.common.helpers import validate_html_content # Added validate_html_content

# Initialize a basic logger for startup. It will be reconfigured by setup_logging.
logger = logging.getLogger(__name__)

# --- Constants ---
NEXT_STEP_EXTRACT_INITIAL_METADATA_TOPIC_NAME = "extract-initial-metadata-topic"
RETRY_TOPIC_NAME = "retry-pipeline" # For publishing "no data found" events

# Default User-Agent for HTTP requests
DEFAULT_USER_AGENT = 'GoogleCloudFunction-DiscoverMainURLs/1.1'

# --- Helper Functions ---

def fetch_page_content_via_requests(url, timeout=30, logger_instance=None, user_agent=DEFAULT_USER_AGENT):
    """Fetches HTML content from a URL using the requests library (static)."""
    active_logger = logger_instance if logger_instance else logger
    try:
        response = requests.get(url, timeout=timeout, headers={'User-Agent': user_agent})
        response.raise_for_status()
        if 'text/html' not in response.headers.get('Content-Type', '').lower():
            active_logger.warning(f"Content-Type for {url} is not HTML: {response.headers.get('Content-Type')}")
            # Still return content, might be parsable or an error page
        return validate_html_content(response.content, active_logger) # Validate and decode
    except requests.exceptions.RequestException as e:
        active_logger.error(f"Failed to fetch URL {url} with requests: {e}")
        return None

def fetch_page_content_dynamically(url, project_config, logger_instance, interactions=None):
    """
    Fetches HTML content using the web-renderer Cloud Run service for dynamic content.
    Supports interactions like clicking accordion triggers.
    """
    active_logger = logger_instance if logger_instance else logger
    cloud_run_scraper_url = project_config.get("web_renderer_url")  # Use web_renderer_url from config
    user_agent = project_config.get("dynamic_scraper_user_agent", DEFAULT_USER_AGENT)
    timeout_for_cloud_run_call = project_config.get("dynamic_fetch_cloud_run_timeout", 60)

    if not cloud_run_scraper_url:
        active_logger.warning(f"Dynamic fetching requested for {url}, but 'web_renderer_url' not configured. Falling back to static fetch.")
        return fetch_page_content_via_requests(url, project_config.get("discovery_request_timeout", 30), active_logger, user_agent)

    # Map interaction selectors to web-renderer format
    renderer_interactions = []
    if interactions:
        for interaction in interactions:
            if interaction.get("type") == "click_all_visible" and interaction.get("selector"):
                renderer_interactions.append({
                    "type": "click",
                    "selector": interaction["selector"]
                })
    else:
        # Default interactions for accordions if none provided
        renderer_interactions = [
            {"type": "click", "selector": "button[aria-expanded='false']"},
            {"type": "click", "selector": ".accordion-button.collapsed"}
        ]

    payload = {
        "url": url,
        "user_agent": user_agent,
        "timeout": project_config.get("dynamic_fetch_render_timeout", 45000),  # 45 seconds in ms
        "max_wait": 10,  # Wait up to 10 seconds for dynamic content
        "expand_accordions": True,  # Enable accordion expansion
        "trigger_lazy_loading": True,  # Handle lazy-loaded content
        "include_metadata": True,  # Get interaction details
        "interactions": renderer_interactions  # Custom interactions
    }
    headers = {"Content-Type": "application/json"}

    try:
        active_logger.info(f"Calling web-renderer for {url} with payload: {payload}")
        response = requests.post(cloud_run_scraper_url, json=payload, headers=headers, timeout=timeout_for_cloud_run_call)
        response.raise_for_status()

        response_json = response.json()
        html_content = response_json.get("html")
        if not html_content:
            active_logger.error(f"No HTML content in web-renderer response for {url}: {response_json}")
            return None

        active_logger.info(f"Successfully fetched dynamic content for {url}. Interactions: {response_json.get('interactions', [])}")
        return validate_html_content(html_content, active_logger)
    except requests.exceptions.RequestException as e:
        active_logger.error(f"Failed to call web-renderer for {url}: {e}")
        return None
    except (json.JSONDecodeError, KeyError) as e:
        active_logger.error(f"Invalid response from web-renderer for {url}: {e}. Response: {response.text[:500]}")
        return None


def analyze_category_page_with_ai(html_content, project_config, gcp_project_id, main_category_url, logger_instance=None):
    """
    Uses Vertex AI (Gemini) to analyze category page HTML and extract selectors.
    Now includes asking for interaction selectors for dynamic content.
    """
    active_logger = logger_instance if logger_instance else logger
    ai_model_cfg = project_config.get("ai_model_config", {})
    model_id = ai_model_cfg.get("discovery_model_id", "gemini-2.0-flash-lite-001") # Updated model if desired
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
            active_logger.warning(f"HTML content for {main_category_url} was truncated from {len(html_content)} to {max_html_len} for AI.")

        prompt = f"""
        Analyze the HTML from a category/listing page: {main_category_url}
        HTML Snippet (potentially truncated):
        ```html
        {truncated_html}
        ```
        Your task is to identify:
        1.  `main_url_selectors`: CSS selectors for links to individual articles/products. Prioritize robust selectors. Avoid navigation/ads.
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
        If no main URL selectors, use empty list. If no pagination, use null for `pagination_selector`.
        """

        generation_config_params = project_config.get("ai_discovery_generation_config", {
            "temperature": 0.2,
            "top_p": 0.8,
            "top_k": 20,
            "max_output_tokens": 2048 # Increased for potentially more selectors
        })
        generation_config_params["response_mime_type"] = "application/json" # Essential for Gemini
        
        generation_config_obj = GenerationConfig(**generation_config_params)


        active_logger.info(f"Sending request to Vertex AI (model: {model_id}) for category page analysis: {main_category_url}")
        response = model.generate_content(
            [Part.from_text(prompt)], # Ensure prompt is passed as a Part object if generate_content expects a list
            generation_config=generation_config_obj
        )

        if response.candidates and response.candidates[0].content.parts:
            response_text = response.candidates[0].content.parts[0].text
            active_logger.info(f"AI analysis raw response for {main_category_url}: {response_text}")
            try:
                ai_derived_config = json.loads(response_text)
                # Validate structure
                if isinstance(ai_derived_config.get("main_url_selectors"), list) and \
                   (ai_derived_config.get("pagination_selector") is None or isinstance(ai_derived_config.get("pagination_selector"), str)) and \
                   isinstance(ai_derived_config.get("category_page_metadata"), dict) and \
                   isinstance(ai_derived_config.get("interaction_selectors_for_hidden_content", []), list): # Check new field
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
    """Extracts hrefs from HTML using a list of CSS selectors."""
    active_logger = logger_instance if logger_instance else logger
    if not html_content or not selectors:
        return set()

    soup = BeautifulSoup(html_content, 'html.parser')
    extracted_urls = set()

    for selector in selectors:
        if not selector or not isinstance(selector, str): # Skip empty or invalid selectors
            active_logger.warning(f"Skipping invalid selector: {selector}")
            continue
        try:
            elements = soup.select(selector)
            for element in elements:
                if element.name == 'a' and element.has_attr('href'):
                    href = element['href'].strip()
                    # Extended exclusion list
                    if href and not href.startswith(('#', 'javascript:', 'mailto:', 'tel:')) and href.strip() != '/':
                        absolute_url = urljoin(base_url, href)
                        # Basic validation of the formed absolute URL
                        parsed_abs_url = urlparse(absolute_url)
                        if parsed_abs_url.scheme and parsed_abs_url.netloc:
                            extracted_urls.add(absolute_url)
                        else:
                            active_logger.debug(f"Skipping invalid absolute URL '{absolute_url}' from href '{href}' and base '{base_url}' with selector '{selector}'.")
                # Allow extracting from other elements if href is not the target (though less common for URLs)
                elif element.has_attr('data-href'): # Example for links stored in data attributes
                    href = element['data-href'].strip()
                    if href and not href.startswith(('#', 'javascript:', 'mailto:', 'tel:')):
                        absolute_url = urljoin(base_url, href)
                        extracted_urls.add(absolute_url)

        except Exception as e: # More specific exceptions can be caught if needed
            active_logger.warning(f"Error applying selector '{selector}' on base {base_url}: {e}")
    return extracted_urls

# --- Main Cloud Function ---

@functions_framework.cloud_event
def discover_main_urls(cloud_event):
    """
    Cloud Function to discover MainURLs from a category page.
    Supports static and dynamic fetching based on configuration or retry payload.
    """
    global logger # Allow logger to be updated by setup_logging

    pubsub_message_data = base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8")
    message_payload = json.loads(pubsub_message_data)
    message_id = cloud_event.data["message"].get("messageId", "N/A")

    customer = message_payload.get("customer")
    project_config_name = message_payload.get("project")
    initial_category_url = message_payload.get("category_url")

    # Parameters that might come from a retry_pipeline suggestion
    use_dynamic_fetching_override = message_payload.get("use_dynamic_fetching", False)
    interaction_selectors_override = message_payload.get("interaction_selectors_for_dynamic_fetch")
    override_main_url_selectors = message_payload.get("override_main_url_selectors")
    override_pagination_selector = message_payload.get("override_pagination_selector")


    # Setup logger first
    logger = setup_logging(customer, project_config_name)
    active_logger = logger # Use the configured logger

    active_logger.info(f"Received message {message_id} for discover_main_urls. Customer: {customer}, Project: {project_config_name}, Category URL: {initial_category_url}. Dynamic fetch override: {use_dynamic_fetching_override}")

    if not all([customer, project_config_name, initial_category_url]):
        active_logger.error("Missing required fields in Pub/Sub message: customer, project, or category_url.")
        return {'status': 'error', 'message': 'Missing required fields.'} # Ack the message

    publisher_client = None # Initialize for potential use in error reporting

    try:
        customer_config = load_customer_config(customer)
        gcp_project_id = customer_config.get("gcp_project_id", os.environ.get("GCP_PROJECT"))

        if not gcp_project_id:
            active_logger.error("GCP Project ID not configured.")
            return {'status': 'error', 'message': 'GCP Project ID not configured.'}

        db = firestore.Client(project=gcp_project_id, database=customer_config.get("firestore_database_id", "(default)"))
        project_config = load_dynamic_site_config(db, project_config_name, active_logger)

        publisher_client = pubsub_v1.PublisherClient() # Initialize now for all paths
        output_topic_path = publisher_client.topic_path(gcp_project_id, NEXT_STEP_EXTRACT_INITIAL_METADATA_TOPIC_NAME)
        retry_topic_path = publisher_client.topic_path(gcp_project_id, RETRY_TOPIC_NAME)


        max_pagination_depth = project_config.get("max_pagination_depth", 5)
        request_timeout = project_config.get("discovery_request_timeout", 30) # For static requests
        all_discovered_main_urls = set()
        processed_page_urls = set()

        current_page_url = initial_category_url
        current_depth = 0
        first_page_ai_derived_config = None # Store AI results from the first page

        # Determine fetch strategy: dynamic if overridden, or if configured by default for discovery
        default_discovery_fetch_dynamic = project_config.get("discovery_fetch_dynamic_by_default", False)
        use_dynamic_fetching = use_dynamic_fetching_override or default_discovery_fetch_dynamic
        
        # Interaction selectors from AI or override
        interaction_selectors_for_dynamic_page = interaction_selectors_override or []


        while current_page_url and current_depth < max_pagination_depth and current_page_url not in processed_page_urls:
            active_logger.info(f"Processing page (Depth {current_depth + 1}): {current_page_url}. Dynamic fetch: {use_dynamic_fetching}")
            processed_page_urls.add(current_page_url)

            html_content = None
            if use_dynamic_fetching:
                # If AI on first page suggests interactions, use them
                interactions_for_current_page = interaction_selectors_for_dynamic_page
                if current_depth == 0 and first_page_ai_derived_config: # check if AI ran and gave interactions
                    interactions_from_ai = first_page_ai_derived_config.get("interaction_selectors_for_hidden_content", [])
                    if interactions_from_ai: # Prioritize AI suggested interactions for the page it analyzed
                        interactions_for_current_page = [{'type': 'click_all_visible', 'selector': sel} for sel in interactions_from_ai]
                        active_logger.info(f"Using AI-suggested interactions for dynamic fetch: {interactions_for_current_page}")
                
                html_content = fetch_page_content_dynamically(current_page_url, project_config, active_logger, interactions=interactions_for_current_page)
            
            if not html_content: # Fallback or primary static fetch
                 active_logger.info(f"Falling back to static fetch for {current_page_url} (or dynamic fetch failed).")
                 html_content = fetch_page_content_via_requests(current_page_url, timeout=request_timeout, logger_instance=active_logger)

            if not html_content:
                active_logger.warning(f"No HTML content fetched for {current_page_url}. Stopping pagination for this branch.")
                break # Stop this pagination path

            main_url_selectors_to_use = []
            pagination_selector_to_use = None
            category_page_metadata = {}
            
            # --- Selector Strategy ---
            if override_main_url_selectors:
                main_url_selectors_to_use = override_main_url_selectors
                active_logger.info(f"Using overridden main_url_selectors from retry payload: {main_url_selectors_to_use}")
            if override_pagination_selector:
                pagination_selector_to_use = override_pagination_selector
                active_logger.info(f"Using overridden pagination_selector from retry payload: {pagination_selector_to_use}")


            # Only run live AI analysis for the first page if not using overrides from retry
            if current_depth == 0 and not (override_main_url_selectors and override_pagination_selector):
                ai_analysis_result = analyze_category_page_with_ai(html_content, project_config, gcp_project_id, current_page_url, active_logger)
                if ai_analysis_result:
                    first_page_ai_derived_config = ai_analysis_result # Store for reuse
                    main_url_selectors_to_use = ai_analysis_result.get("main_url_selectors", [])
                    pagination_selector_to_use = ai_analysis_result.get("pagination_selector")
                    category_page_metadata = ai_analysis_result.get("category_page_metadata", {})
                    # If AI suggests interactions and we are not already overridden to use specific ones
                    if not interaction_selectors_for_dynamic_page and ai_analysis_result.get("interaction_selectors_for_hidden_content"):
                        interaction_selectors_for_dynamic_page = ai_analysis_result.get("interaction_selectors_for_hidden_content")
                        # If dynamic fetching wasn't already on, but AI suggests interactions, turn it on for next time (or re-fetch current if logic allows)
                        if not use_dynamic_fetching:
                             active_logger.info(f"AI suggested interactions ({interaction_selectors_for_dynamic_page}), consider enabling dynamic fetching for subsequent pages or retries.")
                             # For simplicity, we'll use these interactions on the *next* dynamic fetch if it happens.
                             # To re-fetch current page dynamically, more complex logic flow needed here.

                    active_logger.info(f"Using live AI-derived selectors for first page {current_page_url}.")

            # If still no selectors (e.g. AI failed, or not first page, or overrides weren't there)
            if not main_url_selectors_to_use:
                # Try to reuse from first page's AI analysis if available
                if first_page_ai_derived_config and first_page_ai_derived_config.get("main_url_selectors"):
                    active_logger.info(f"Reusing AI-derived selectors from first page analysis for {current_page_url}.")
                    main_url_selectors_to_use = first_page_ai_derived_config.get("main_url_selectors", [])
                    pagination_selector_to_use = first_page_ai_derived_config.get("pagination_selector") # Ensure this is also reused
                    category_page_metadata = first_page_ai_derived_config.get("category_page_metadata", {}) # And metadata

                # Fallback to site-wide selectors from dynamic config (e.g., from analyze_website_schema)
                elif project_config.get("discovery_selectors") and project_config.get("discovery_selectors").get("main_url_selectors"):
                    site_wide_selectors_config = project_config.get("discovery_selectors", {})
                    main_url_selectors_to_use = site_wide_selectors_config.get("main_url_selectors", [])
                    pagination_selector_to_use = site_wide_selectors_config.get("pagination_selector")
                    if not category_page_metadata: # Only use site-wide if page-specific wasn't set from AI
                         category_page_metadata = site_wide_selectors_config.get("category_page_metadata", {})
                    active_logger.info(f"Using site-wide AI-derived selectors from dynamic config for {current_page_url}.")
                
                # Final fallback to static selectors in the project config file
                else:
                    fallback_config = project_config.get("discovery_fallback_selectors", {})
                    main_url_selectors_to_use = fallback_config.get("main_url_selectors", [])
                    pagination_selector_to_use = fallback_config.get("pagination_selector")
                    if not category_page_metadata:
                        category_page_metadata = fallback_config.get("category_page_metadata", {})
                    active_logger.warning(f"Using static fallback selectors for {current_page_url}.")
            
            if not main_url_selectors_to_use:
                active_logger.error(f"No main URL selectors available for {current_page_url} after all strategies. Cannot extract links.")
                break # Stop this pagination path

            # --- Extract MainURLs ---
            page_main_urls = extract_links_using_selectors(html_content, main_url_selectors_to_use, current_page_url, active_logger)
            newly_discovered_urls = page_main_urls - all_discovered_main_urls
            
            if newly_discovered_urls:
                all_discovered_main_urls.update(newly_discovered_urls)
                active_logger.info(f"Found {len(page_main_urls)} links on {current_page_url}, {len(newly_discovered_urls)} are new.")

                for main_url in newly_discovered_urls:
                    parsed_url = urlparse(main_url)
                    if not parsed_url.scheme or not parsed_url.netloc or len(main_url) < 15: # Basic sanity check
                        active_logger.warning(f"Skipping potentially invalid or short MainURL: {main_url}")
                        continue

                    payload_for_next_step = {
                        "customer": customer,
                        "project": project_config_name,
                        "main_url": main_url,
                        "category_url": initial_category_url, # Original category URL
                        "source_page_url": current_page_url,  # Page where this main_url was found
                        "category_page_metadata": category_page_metadata # Metadata from category page AI analysis
                    }
                    try:
                        publisher_client.publish(output_topic_path, json.dumps(payload_for_next_step).encode("utf-8"))
                        active_logger.debug(f"Published MainURL {main_url} to {NEXT_STEP_EXTRACT_INITIAL_METADATA_TOPIC_NAME}")
                    except Exception as e_pub:
                        active_logger.error(f"Failed to publish MainURL {main_url}: {e_pub}")
            else:
                 active_logger.info(f"No new main URLs discovered on {current_page_url} with selectors: {main_url_selectors_to_use}")


            # --- Handle Pagination ---
            next_page_url = None
            if pagination_selector_to_use: # Ensure this is also sourced correctly from overrides or AI
                soup = BeautifulSoup(html_content, 'html.parser')
                try:
                    pagination_element = soup.select_one(pagination_selector_to_use)
                    if pagination_element and pagination_element.name == 'a' and pagination_element.has_attr('href'):
                        href = pagination_element['href'].strip()
                        if href and not href.startswith(('#', 'javascript:', 'mailto:', 'tel:')) and href.strip() != '/':
                            next_page_url = urljoin(current_page_url, href)
                            active_logger.info(f"Found next page link: {next_page_url} using selector '{pagination_selector_to_use}'")
                        else:
                            active_logger.info(f"Pagination link by selector '{pagination_selector_to_use}' has invalid href: '{href}'")
                    else:
                        active_logger.info(f"Pagination selector '{pagination_selector_to_use}' did not match a valid <a> tag with href.")
                except Exception as e_select:
                     active_logger.warning(f"Error applying pagination selector '{pagination_selector_to_use}': {e_select}")
            else:
                active_logger.info(f"No pagination selector defined or found for {current_page_url}.")

            current_page_url = next_page_url
            current_depth += 1
            if current_page_url:
                time.sleep(project_config.get("discovery_pagination_delay_ms", 1000) / 1000.0) # Delay in seconds

        # After loop finishes for the initial_category_url
        if not all_discovered_main_urls and processed_page_urls: # Processed pages but found no items
            active_logger.warning(f"No main URLs discovered for initial category URL {initial_category_url} after processing {len(processed_page_urls)} page(s). This might indicate an issue or an empty category.")
            
            if project_config.get("retry_on_no_main_urls_found", False):
                no_data_payload = {
                    "customer": customer,
                    "project": project_config_name,
                    "identifier": generate_url_hash(f"no_main_urls_found_{initial_category_url}"),
                    "category_url": initial_category_url,
                    "error_message": f"No main URLs discovered for category {initial_category_url}. Processed {len(processed_page_urls)} pages. Consider dynamic fetching or selector review.",
                    "stage": "discover_main_urls_no_data",
                    "retry_count": message_payload.get("retry_count", 0) +1, # Increment from original if retrying this state
                    "item_data_snapshot": {"category_url": initial_category_url, "project_config_name": project_config_name, "last_processed_page_count": len(processed_page_urls)}
                }
                try:
                    publisher_client.publish(retry_topic_path, json.dumps(no_data_payload).encode("utf-8"))
                    active_logger.info(f"Published 'no_data_found' event to {RETRY_TOPIC_NAME} for {initial_category_url}")
                except Exception as e_pub_err:
                    active_logger.error(f"Failed to publish 'no_data_found' event for {initial_category_url}: {e_pub_err}")

        active_logger.info(f"Finished discovery for category {initial_category_url}. Total unique MainURLs found: {len(all_discovered_main_urls)}. Processed {len(processed_page_urls)} unique category pages.")
        return {'status': 'success', 'category_url': initial_category_url, 'main_urls_found_count': len(all_discovered_main_urls), "category_pages_processed": len(processed_page_urls)}

    except Exception as e:
        active_logger.error(f"Critical error in discover_main_urls for {initial_category_url}: {e}", exc_info=True)
        # It's generally better to let Pub/Sub retry the original message for critical errors,
        # or publish to a dead-letter queue if configured.
        # If you have a custom retry_pipeline for such critical errors, you could publish there.
        # For now, re-raise to signal failure to Cloud Functions.
        raise