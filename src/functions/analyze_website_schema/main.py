# src/functions/analyze_website_schema/main.py

import json
import base64
import logging
import os
import random
from urllib.parse import urljoin
import re # Not explicitly used in this version, but often useful for regex in selectors if needed by AI

import requests
from bs4 import BeautifulSoup
from google.cloud import firestore, pubsub_v1, storage
import vertexai
from vertexai.generative_models import GenerativeModel, Part, GenerationConfig # Corrected import for Part
import functions_framework
import pandas as pd
from io import StringIO
from datetime import datetime # Imported for schema_data timestamp

# Import common modules from your pipeline
from src.common.utils import setup_logging
from src.common.config import load_customer_config, load_project_config # Assuming load_project_config exists
from src.common.helpers import validate_html_content, sanitize_error_message

# Initialize logger
logger = logging.getLogger(__name__)

# Constants
NEXT_STEP_INGEST_CATEGORY_URLS_TOPIC_NAME = "start-ingest-category-urls-topic"
MAX_HTML_FOR_PROMPT = 3000000  # Max characters for LLM prompt (Note: Check model limits)
SAMPLE_SIZE = 10  # Number of category URLs to sample for analysis
REQUEST_TIMEOUT = 30  # Seconds for HTTP requests
MAX_FIRESTORE_RETRIES = 3
RETRY_BACKOFF = 2  # Seconds
RETRY_TOPIC_NAME = "retry-pipeline" # For publishing retry messages

def fetch_page_content(url, project_config, timeout=REQUEST_TIMEOUT, logger_instance=None):
    """Enhanced to use web-renderer service consistently with other functions."""
    active_logger = logger_instance or logger
    
    # Try web-renderer first
    renderer_url = project_config.get('web_renderer_url')
    
    if renderer_url:
        try:
            # Prepare payload for web-renderer
            payload = {
                "url": url,
                "user_agent": "GoogleCloudFunction-AnalyzeWebsiteSchema/1.0",
                "include_metadata": True, # To get interaction logs if needed
                "expand_accordions": True,
                "trigger_lazy_loading": True,
                "timeout": timeout * 1000,  # Convert seconds to milliseconds for renderer
                "max_wait": 10  # Wait up to 10 seconds for dynamic content in renderer
            }

            active_logger.debug(f"Calling web-renderer at {renderer_url} for {url}")
            # Call web-renderer service
            response = requests.post(
                renderer_url,
                json=payload,
                timeout=timeout, # Timeout for the requests.post call itself
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()

            # Parse response
            result = response.json()
            if "html" not in result:
                active_logger.error(f"No HTML returned from web-renderer for {url}: {result}")
                return None

            html_content = result.get("html")
            # Basic validation of HTML content
            if not validate_html_content(html_content, active_logger, min_length=100):
                active_logger.warning(f"HTML from web-renderer for {url} seems too short or invalid.")
                return None

            active_logger.info(f"Fetched rendered HTML for {url} via web-renderer, interactions: {result.get('interactions', [])}")
            return html_content

        except requests.exceptions.Timeout:
            active_logger.warning(f"Web-renderer request timed out for {url}. Falling back.")
        except requests.exceptions.RequestException as e:
            active_logger.warning(f"Web-renderer failed for {url}: {e}. Falling back to static fetch.")
        except Exception as e: # Catch any other unexpected errors from renderer interaction
            active_logger.warning(f"Unexpected error with web-renderer for {url}: {e}. Falling back.")

    # Fallback to static fetch
    active_logger.info(f"Attempting static fetch for {url}.")
    try:
        response = requests.get(
            url,
            timeout=timeout,
            headers={'User-Agent': 'GoogleCloudFunction-AnalyzeWebsiteSchema/1.0'}
        )
        response.raise_for_status()
        
        if 'text/html' not in response.headers.get('Content-Type', '').lower():
            active_logger.warning(f"Non-HTML content at {url} (static fetch): {response.headers.get('Content-Type')}")
            return None
            
        # Use validate_html_content which might decode and check length
        html_content = validate_html_content(response.content, active_logger, min_length=100)
        if html_content:
            active_logger.info(f"Static fetch succeeded for {url}")
            return html_content
        else:
            active_logger.warning(f"Static fetch for {url} resulted in invalid or too short HTML.")
            return None
            
    except requests.exceptions.Timeout:
        active_logger.error(f"Static fetch also timed out for {url}.")
        return None
    except requests.RequestException as e:
        active_logger.error(f"Static fetch also failed for {url}: {e}")
        return None


def extract_main_urls_from_category_page(html_content, base_url, logger_instance=None):
    """Enhanced URL extraction with better heuristics for different site types."""
    active_logger = logger_instance or logger
    if not html_content:
        active_logger.debug(f"No HTML content provided to extract_main_urls_from_category_page for base: {base_url}")
        return []
    
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        main_urls = set()
        
        # Enhanced selectors for different types of sites
        selectors = [
            # Standard content selectors
            'article a', 'div.item a', 'h2 a', 'h3 a', 'li a',
            
            # Government/regulatory site patterns - make them more specific if possible
            'a[href*="/report"]', 'a[href*="/guide"]', 'a[href*="/consultation"]',
            'a[href*="/media-release"]', 'a[href*="/document"]', 'a[href*="/instrument"]',
            'a[href*="/law"]', 'a[href*="/article"]', 'a[href*="/publication"]',
            
            # Table-based content (common in government sites)
            'table td a', 'table th a',
            
            # List-based navigation - often contain relevant links
            'ul.content-list a', 'ol.content-list a', 'div.content-list a',
            
            # Common CMS patterns
            '.post-title a', '.entry-title a', '.content-title a',
            '.document-link', '.resource-link',
            
            # Example: ASIC-specific patterns (if relevant for a customer)
            # 'a[href*="/rep-"]', 'a[href*="/rg-"]', 'a[href*="/cp-"]', 'a[href*="/mr-"]'
        ]
        
        for selector in selectors:
            try:
                for link in soup.select(selector):
                    if link.get('href'):
                        href = link['href'].strip()
                        link_text = link.get_text(strip=True)
                        
                        # Skip navigation, utility, and very short/irrelevant links
                        if (href and 
                            not href.startswith(('#', 'javascript:', 'mailto:', 'tel:')) and
                            href != '/' and # Avoid just linking to homepage if it's not specific
                            len(link_text) > 2 and  # Meaningful link text; adjust as needed
                            not any(skip_word in link_text.lower() for skip_word in [
                                'home', 'contact', 'about', 'privacy', 'terms', 'login', 'register', 
                                'sitemap', 'accessibility', 'careers', 'faq', 'help', 'support',
                                'media centre', 'press office' # More comprehensive skip list
                            ]) and
                            not any(skip_path in href.lower() for skip_path in [
                                '/contact', '/about', '/privacy', '/terms', '/login', '/user/profile' # Skip common non-content paths
                            ])) :
                            
                            absolute_url = urljoin(base_url, href)
                            # Further filter based on URL patterns if needed (e.g. ensure it's not an external ad link)
                            if urljoin(absolute_url, '/') == urljoin(base_url, '/'): # Check if it's on the same domain (simple check)
                                main_urls.add(absolute_url)
            except Exception as e:
                active_logger.debug(f"Error processing selector '{selector}' for {base_url}: {e}")
                continue
        
        # Also look for PDF links as potential main URLs, often very relevant
        pdf_links = soup.find_all('a', href=lambda x: x and x.lower().endswith('.pdf'))
        for pdf_link in pdf_links:
            href = pdf_link.get('href')
            if href:
                absolute_url = urljoin(base_url, href)
                if urljoin(absolute_url, '/') == urljoin(base_url, '/'): # Ensure PDF is from same domain
                     main_urls.add(absolute_url)
        
        active_logger.info(f"Extracted {len(main_urls)} potential main URLs from {base_url}")
        return list(main_urls)
        
    except Exception as e:
        active_logger.error(f"Error extracting main URLs from {base_url}: {e}", exc_info=True)
        return []


def analyze_html_with_ai(html_samples, project_config, gcp_project_id, logger_instance=None):
    """Enhanced AI analysis with updated models and better prompting."""
    active_logger = logger_instance or logger
    ai_config = project_config.get("ai_model_config", {})
    # Ensure a valid default model ID, consider making this more configurable
    model_id = ai_config.get("schema_analysis_model_id", "gemini-2.0-flash") 
    vertex_ai_location = project_config.get("vertex_ai_location", "europe-west1") # Default or from config

    if not html_samples:
        active_logger.warning("No HTML samples provided for AI analysis.")
        return None

    try:
        vertexai.init(project=gcp_project_id, location=vertex_ai_location)
        model = GenerativeModel(model_id)

        # Truncate HTML samples to fit within context window
        # This needs to be smart about overall token limits for the chosen model.
        # Gemini 1.5 Flash has a large context window (1M tokens), so MAX_HTML_FOR_PROMPT might be generous.
        # However, actual token count is what matters, not char count.
        truncated_samples = []
        # A simple character-based truncation might still be necessary for very large pages
        # or if many samples are combined.
        # Consider a more sophisticated token-based truncation if issues arise.
        # MAX_HTML_FOR_PROMPT could be total chars for ALL samples.
        total_chars_processed = 0
        for sample in html_samples:
            if total_chars_processed >= MAX_HTML_FOR_PROMPT:
                active_logger.warning(f"Reached MAX_HTML_FOR_PROMPT ({MAX_HTML_FOR_PROMPT} chars). Skipping further HTML samples for AI.")
                break
            
            remaining_chars = MAX_HTML_FOR_PROMPT - total_chars_processed
            truncated_html = sample["html"][:remaining_chars]
            
            if len(sample["html"]) > remaining_chars:
                active_logger.warning(f"Truncated HTML for {sample['url']} from {len(sample['html'])} to {len(truncated_html)} chars.")
            
            truncated_samples.append({
                "url": sample["url"], 
                "html": truncated_html, 
                "type": sample["type"] # 'category' or 'main'
            })
            total_chars_processed += len(truncated_html)
        
        if not truncated_samples:
            active_logger.error("No HTML samples remain after truncation. Cannot proceed with AI analysis.")
            return None

        # Enhanced prompt for better schema analysis
        samples_json = json.dumps(truncated_samples, indent=2)
        # Ensure the prompt clearly outlines the expected JSON structure for the output
        prompt = f"""
        Analyze the following HTML samples from a website to propose a comprehensive metadata schema, 
        field mappings, XML structure, and discovery/extraction configuration.

        HTML Samples (JSON format, includes URL, HTML content, and type ('category' or 'main')):
        ```json
        {samples_json}
        ```

        Your task is to create a complete configuration for processing this website. 
        Focus on the HTML structure and content patterns observed in the samples.
        Return a single, valid JSON object containing ALL the following sections:

        1.  **metadata_fields**:
            List of metadata fields extractable from the 'main' type documents.
            For each field, provide: `name` (underscore_case), `description`, and a precise 
            `selector` (CSS selector or extraction strategy like "xpath://...", "regex:...", "json_path:...").
            Example: `[{{"name": "document_title", "description": "The main title of the document", "selector": "h1.article-title"}}, ...]`

        2.  **field_mappings**: 
            Mappings for each metadata field. `source` should be the `name` from `metadata_fields`.
            `type` can be "direct", "attribute", "computed", "datetime", etc. Add `attribute_name` if type is "attribute".
            Example: `{{"document_title": {{"source": "document_title", "type": "direct"}}, "publication_date": {{"source": "date_meta_tag", "type": "datetime", "datetime_format": "%Y-%m-%d"}}, ...}}`

        3.  **xml_structure**: 
            Proposed hierarchical XML structure for output.
            Define `root_tag`, `item_tag`, `filename_template` (e.g., using `{{document_id}}`), and `fields`.
            Each field in `fields` should have `tag` (XML tag name), `source` (from `metadata_fields`), and optional `cdata` (boolean).
            Example: `{{ "root_tag": "RegulatoryDocuments", "item_tag": "Document", "filename_template": "{{document_id}}.xml", "fields": [{{"tag": "Title", "source": "document_title", "cdata": true}}, ...] }}`

        4.  **language_and_country**: 
            Detected primary language (e.g., "English", "French") with `language_code` (e.g., "en", "fr") and `country` of origin.
            Example: `{{"language": "English", "language_code": "en", "country": "Australia"}}`

        5.  **discovery_selectors**: 
            CSS selectors for discovering content on 'category' type pages.
            Include:
            -   `main_url_selectors`: A list of CSS selectors to find links to individual 'main' documents.
                Example: `["article.news-item a.title", ".document-list li a"]`
            -   `pagination_selectors`: A list of CSS selectors to find 'next page' links or pagination controls.
                Example: `["a.pagination-next", ".pager li.next a"]`
            -   `item_container_selector` (optional): A CSS selector for the container of each item in a list on category pages, if applicable.

        6.  **heuristic_link_extraction**: 
            Configuration for heuristic-based link discovery if direct selectors are insufficient.
            Include `enabled` (boolean), `content_selectors` (areas to search within), `exclusion_selectors` (areas to ignore),
            `link_text_keywords` (e.g., ["report", "pdf"]), `href_patterns` (regex for hrefs), `min_link_text_len`.
            Example: `{{ "enabled": true, "content_selectors": ["main#content", "article.body"], ... }}`

        7.  **document_type_patterns**: 
            Patterns to classify different types of 'main' documents based on URL, title, or content keywords.
            The key is the document type name, the value is a list of keywords or regex patterns.
            Example: `{{"Media Release": ["/media-releases/", "news/", "announcement"], "Guidance Note": ["/guidance/", "gn-", "guide"]}}`
        
        8.  **example_extraction_targets** (optional but helpful):
            For 2-3 key `metadata_fields` you identified (e.g., title, date, author), provide the exact text content
            you would expect to be extracted from one of the provided 'main' HTML samples, along with the URL of that sample.
            This helps verify the accuracy of your proposed selectors.
            Example: `[{{ "url": "sample_main_url_1", "field": "document_title", "extracted_text": "Example Document Title From Sample 1"}}, ...]`


        Prioritize government/regulatory website patterns if applicable (e.g., for ASIC, FCA, SEC).
        Ensure all field names and keys in your JSON output use underscore_case.
        Selectors must be valid CSS or clearly indicate other types (XPath, Regex).
        If some sections are not applicable or cannot be determined, provide an empty list/object or a note.
        The response MUST be a single, valid JSON object.
        """
        
        generation_config = GenerationConfig(
            temperature=0.1,  # Lower temperature for more factual, less creative responses
            top_p=0.8,        # Nucleus sampling
            top_k=20,         # Top-k sampling
            max_output_tokens=8192, # Max tokens for the model's response
            response_mime_type="application/json" # Expect JSON output directly
        )

        active_logger.info(f"Sending request to Vertex AI (model: {model_id}) for website schema analysis. Prompt size: ~{len(prompt)} chars.")
        
        # Vertex AI call
        response = model.generate_content(
            [Part.from_text(prompt)], # Use Part.from_text for text prompts
            generation_config=generation_config
        )

        if not response.candidates or not response.candidates[0].content.parts:
            active_logger.error("No valid response or candidates from AI model.")
            return None

        # Assuming the response is directly JSON if response_mime_type="application/json" worked
        response_text = response.candidates[0].content.parts[0].text.strip()
        active_logger.info(f"AI analysis raw response received (first 500 chars): {response_text[:500]}...")
        
        try:
            ai_derived_config = json.loads(response_text)
            
            # Basic validation for key sections presence (as defined in the prompt)
            required_keys = [
                "metadata_fields", "field_mappings", "xml_structure",
                "language_and_country", "discovery_selectors", 
                "heuristic_link_extraction", "document_type_patterns"
            ] # "example_extraction_targets" is optional
            
            missing_keys = [key for key in required_keys if key not in ai_derived_config]
            if missing_keys:
                active_logger.warning(f"AI response is missing some expected top-level keys: {missing_keys}. Will proceed but schema might be incomplete.")
                # Optionally, add default empty structures for missing keys to prevent downstream errors
                for key in missing_keys:
                    if key.endswith("_selectors") or key.endswith("_patterns") or key.endswith("_mappings"):
                        ai_derived_config[key] = {} if not key.startswith("discovery") else {"main_url_selectors": [], "pagination_selectors": []}
                    elif key.endswith("_fields"):
                         ai_derived_config[key] = []
                    # Add more specific defaults as needed

            active_logger.info(f"Successfully parsed AI-derived schema config. Top-level keys: {list(ai_derived_config.keys())}")
            return ai_derived_config
            
        except json.JSONDecodeError as e:
            active_logger.error(f"Failed to decode AI JSON response: {e}. Response text was: {response_text}")
            # Potentially try to fix minor JSON issues if possible, or log more details
            return None
        except Exception as e: # Catch other unexpected errors during parsing/validation
            active_logger.error(f"Unexpected error processing AI response: {e}", exc_info=True)
            return None

    except Exception as e:
        active_logger.error(f"Vertex AI analysis request failed: {e}", exc_info=True)
        return None


@functions_framework.cloud_event
def analyze_website_schema(cloud_event):
    """
    Enhanced website schema analysis with consistent web-renderer usage and improved AI prompting.
    Analyzes a sample of category and main URLs to propose a metadata schema using Vertex AI.
    """
    active_logger = logger # Use module-level logger initially
    publisher_client = None # Initialize to None
    gcp_project_id = os.environ.get("GCP_PROJECT") # Global GCP Project ID

    # Declare variables that might be used in `except` block for retry publishing
    customer = "unknown_customer"
    project_config_name = "unknown_project"
    csv_gcs_path = "unknown_csv_path"
    message_payload = {} # Default to empty dict

    try:
        # Decode Pub/Sub message
        if isinstance(cloud_event.data, dict) and "message" in cloud_event.data:
            pubsub_message_data = base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8")
            message_payload = json.loads(pubsub_message_data)
            message_id = cloud_event.data["message"].get("messageId", "N/A")
        else:
            # Handle direct invocation or different event structure if necessary
            active_logger.error("Invalid CloudEvent structure. Missing 'message' or 'data'.")
            raise ValueError("Invalid CloudEvent data structure.")

        customer = message_payload.get("customer")
        project_config_name = message_payload.get("project")
        csv_gcs_path = message_payload.get("csv_gcs_path") # GCS path to a CSV of category URLs

        if not all([customer, project_config_name, csv_gcs_path]):
            active_logger.error(f"Missing required fields in Pub/Sub message {message_id}: customer, project, or csv_gcs_path.")
            raise ValueError("Missing required fields in Pub/Sub message.")

        # Setup contextual logging
        active_logger = setup_logging(customer, project_config_name)
        active_logger.info(f"Processing Pub/Sub message ID {message_id} for schema analysis. Customer: {customer}, Project: {project_config_name}, CSV: {csv_gcs_path}")

        # Load configurations
        customer_config = load_customer_config(customer_id)
        project_config = load_dynamic_site_config(db, project_config_name, active_logger)

        # Determine GCP Project ID (prefer customer-specific if available)
        gcp_project_id_customer = customer_config.get("gcp_project_id")
        if gcp_project_id_customer:
            gcp_project_id = gcp_project_id_customer
        
        if not gcp_project_id:
            active_logger.error("GCP Project ID could not be determined from environment or customer config.")
            raise ValueError("GCP Project ID not configured.")
        active_logger.info(f"Using GCP Project ID: {gcp_project_id}")


        # Initialize Cloud clients
        storage_client = storage.Client(project=gcp_project_id)
        bucket_name = project_config.get("gcs_bucket_main", project_config.get("gcs_bucket")) # Prefer specific or fallback
        if not bucket_name:
            active_logger.error("GCS bucket name not configured in project_config.")
            raise ValueError("GCS bucket name not configured.")

        if not csv_gcs_path.startswith(f"gs://{bucket_name}/"):
            active_logger.error(f"CSV GCS path '{csv_gcs_path}' does not match configured bucket 'gs://{bucket_name}/'.")
            # This might be a strict check; consider if cross-bucket paths are ever valid.
            # For now, assume it must be within the main project bucket.
            # raise ValueError("CSV GCS path is not within the configured project bucket.")
            # Soften this: if the bucket name in the path is different, parse it.
            parsed_gcs_path_bucket = csv_gcs_path.split('/')[2]
            if parsed_gcs_path_bucket != bucket_name:
                 active_logger.warning(f"CSV GCS path bucket '{parsed_gcs_path_bucket}' differs from project main bucket '{bucket_name}'. Using path's bucket.")
                 bucket_name = parsed_gcs_path_bucket # Use the bucket from the GCS path

        bucket = storage_client.bucket(bucket_name)
        blob_name = csv_gcs_path.replace(f"gs://{bucket_name}/", "")
        blob = bucket.blob(blob_name)

        if not blob.exists():
            active_logger.error(f"CSV file not found at gs://{bucket_name}/{blob_name}")
            raise FileNotFoundError(f"CSV file not found at gs://{bucket_name}/{blob_name}")

        db = firestore.Client(project=gcp_project_id, database=project_config.get("firestore_database_id", "(default)"))
        publisher_client = pubsub_v1.PublisherClient()

        # Read CSV and sample category URLs
        csv_content = blob.download_as_text()
        csv_file = StringIO(csv_content) # Use StringIO for pandas
        df = pd.read_csv(csv_file)
        if "url" not in df.columns: # Ensure the CSV has a 'url' column
            active_logger.error("Input CSV file does not contain a 'url' column.")
            raise ValueError("CSV file missing 'url' column.")

        category_urls = df["url"].dropna().unique().tolist() # Get unique, non-null URLs
        if not category_urls:
            active_logger.error("No valid URLs found in the provided CSV file.")
            raise ValueError("No valid URLs found in CSV.")

        # Sample URLs for analysis (both category and main document pages)
        sample_size_cat = min(SAMPLE_SIZE, len(category_urls))
        sample_category_urls = random.sample(category_urls, sample_size_cat)
        active_logger.info(f"Sampled {len(sample_category_urls)} category URLs for analysis: {sample_category_urls[:3]}...")

        html_samples = []
        # Aim for a certain number of 'main' document samples too, if possible.
        total_main_samples_target = project_config.get("schema_analysis_main_samples_target", 5) 
        # Total samples for AI (category + main)
        total_html_samples_cap = project_config.get("schema_analysis_total_samples_cap", 15) 
        
        main_urls_collected_overall = set()

        for cat_url in sample_category_urls:
            if len(html_samples) >= total_html_samples_cap:
                active_logger.info(f"Reached total HTML sample cap ({total_html_samples_cap}). Stopping collection.")
                break

            active_logger.debug(f"Fetching category page: {cat_url}")
            html_content = fetch_page_content(cat_url, project_config, logger_instance=active_logger)
            if html_content:
                html_samples.append({"url": cat_url, "html": html_content, "type": "category"})
                active_logger.info(f"Added category page sample ({len(html_samples)}/{total_html_samples_cap}): {cat_url}")
                
                # Extract and sample main URLs from this category page
                # Limit the number of main URLs to check per category to avoid excessive fetching
                main_urls_from_cat = extract_main_urls_from_category_page(html_content, cat_url, active_logger)
                
                # Add to overall set to fetch unique main URLs later if still under_target
                for m_url in main_urls_from_cat:
                    if m_url not in main_urls_collected_overall:
                         main_urls_collected_overall.add(m_url)

        # Now, fetch some unique main URLs if we haven't hit the cap
        num_main_samples_fetched = 0
        if main_urls_collected_overall and len(html_samples) < total_html_samples_cap:
            main_urls_to_fetch_sample = random.sample(
                list(main_urls_collected_overall), 
                min(len(main_urls_collected_overall), total_main_samples_target, total_html_samples_cap - len(html_samples))
            )
            active_logger.info(f"Attempting to fetch {len(main_urls_to_fetch_sample)} main document samples.")
            for main_url in main_urls_to_fetch_sample:
                if len(html_samples) >= total_html_samples_cap: break
                active_logger.debug(f"Fetching main document page: {main_url}")
                main_html = fetch_page_content(main_url, project_config, logger_instance=active_logger)
                if main_html:
                    html_samples.append({"url": main_url, "html": main_html, "type": "main"})
                    num_main_samples_fetched +=1
                    active_logger.info(f"Added main page sample ({len(html_samples)}/{total_html_samples_cap}): {main_url}")
        
        active_logger.info(f"Collected a total of {len(html_samples)} HTML samples: "
                           f"{sum(1 for s in html_samples if s['type'] == 'category')} category, "
                           f"{sum(1 for s in html_samples if s['type'] == 'main')} main.")

        if len(html_samples) < project_config.get("schema_analysis_min_samples_required", 2): # Need at least a few samples
            active_logger.error(f"Insufficient HTML samples collected ({len(html_samples)}). Need at least {project_config.get('schema_analysis_min_samples_required', 2)} for analysis.")
            raise RuntimeError("Could not collect enough HTML samples for schema analysis.")

        # Analyze with Vertex AI
        ai_schema = analyze_html_with_ai(html_samples, project_config, gcp_project_id, active_logger)
        if not ai_schema: # analyze_html_with_ai should return None on failure
            active_logger.error("AI schema analysis failed or produced no valid schema output.")
            raise RuntimeError("AI schema analysis failed to produce a valid schema.")

        # Store schema in Firestore (merge with existing project config to not overwrite other settings)
        project_doc_ref = db.collection(project_config.get("firestore_projects_collection", "projects")).document(project_config_name)
        
        firestore_update_data = {
            "ai_derived_schema": ai_schema, # Store the AI schema under a specific key
            "schema_last_analyzed_timestamp": firestore.SERVER_TIMESTAMP,
            "schema_analysis_source_csv": csv_gcs_path,
            "schema_analysis_samples_count": len(html_samples),
            "last_schema_analysis_status": "success"
        }

        for attempt in range(MAX_FIRESTORE_RETRIES):
            try:
                project_doc_ref.set(firestore_update_data, merge=True) # Use merge=True
                active_logger.info(f"Stored/merged AI-derived schema in Firestore for project '{project_config_name}'.")
                break 
            except Exception as e_fs:
                active_logger.warning(f"Firestore set (attempt {attempt + 1}/{MAX_FIRESTORE_RETRIES}) failed: {e_fs}")
                if attempt < MAX_FIRESTORE_RETRIES - 1:
                    import time # Import time for sleep
                    time.sleep(RETRY_BACKOFF * (2 ** attempt)) # Exponential backoff
                else:
                    active_logger.error(f"Max retries reached for Firestore set operation: {e_fs}")
                    raise # Re-raise the last exception if all retries fail

        # Store schema in GCS for reference and backup
        # Use a more structured path, e.g., schemas/{project_name}/{timestamp_or_id}.json
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        schema_gcs_filename = f"schema_analysis_{project_config_name}_{timestamp_str}_{message_id}.json"
        schema_gcs_path_rel = os.path.join(
            project_config.get("gcs_schema_backups_path", "system/schema_backups"), 
            schema_gcs_filename
        )
        schema_blob = bucket.blob(schema_gcs_path_rel)
        
        # Include metadata about the generation process in the GCS backup
        schema_backup_data = {
            "ai_derived_schema": ai_schema,
            "generation_metadata": {
                "timestamp_utc": datetime.utcnow().isoformat() + "Z",
                "source_csv_gcs_path": csv_gcs_path,
                "samples_analyzed_count": len(html_samples),
                "category_urls_in_input_csv": len(category_urls),
                "sampled_category_urls_count": len(sample_category_urls),
                "fetched_main_document_samples_count": num_main_samples_fetched,
                "ai_model_used": project_config.get("ai_model_config", {}).get("schema_analysis_model_id", "gemini-2.0-flash"),
                "triggering_message_id": message_id
            }
        }
        schema_blob.upload_from_string(
            json.dumps(schema_backup_data, indent=2), 
            content_type="application/json"
        )
        full_schema_gcs_path = f"gs://{bucket_name}/{schema_gcs_path_rel}"
        active_logger.info(f"Stored schema analysis backup at {full_schema_gcs_path}")

        # Trigger next step in the pipeline (e.g., ingest_category_urls to discover main URLs based on new schema)
        next_step_topic_name = project_config.get("topics",{}).get("start_ingest_category_urls", NEXT_STEP_INGEST_CATEGORY_URLS_TOPIC_NAME)
        next_step_payload = {
            "customer": customer,
            "project": project_config_name,
            "csv_gcs_path": csv_gcs_path, # Pass along the original CSV path
            "schema_analysis_gcs_backup_path": full_schema_gcs_path # Inform next step about the backup
        }
        topic_path = publisher_client.topic_path(gcp_project_id, next_step_topic_name)
        future = publisher_client.publish(topic_path, json.dumps(next_step_payload).encode("utf-8"))
        future.result() # Wait for publish to complete
        active_logger.info(f"Published message to topic '{next_step_topic_name}' to trigger next pipeline stage for '{project_config_name}'.")

        return {
            "status": "success",
            "project": project_config_name,
            "message": f"Schema analysis completed. AI-derived schema stored. Triggered next step.",
            "schema_backup_gcs_path": full_schema_gcs_path,
            "samples_analyzed_total": len(html_samples)
        }

    except FileNotFoundError as fnf_e:
        active_logger.error(f"File not found error: {fnf_e}", exc_info=True)
        return {"status": "error", "message": f"File not found: {str(fnf_e)}"}
    except ValueError as ve: # Catch config/input validation errors
        active_logger.error(f"Validation or configuration error: {ve}", exc_info=True)
        return {"status": "error", "message": str(ve)}
    except RuntimeError as rte: # Catch specific runtime errors raised within the function
        active_logger.error(f"Runtime error during schema analysis: {rte}", exc_info=True)
        # Fall through to generic exception handling for retry logic if configured
        # but this allows for more specific error messages if not retrying.
        # For retry logic, ensure this exception is re-raised or handled similarly to generic Exception.
        # To ensure retry logic is hit, re-raise or fall through to the generic Exception block.
        # If retry is desired for this, make sure this doesn't return before the generic except block.
        # For simplicity, let's assume RuntimeError should also attempt retry.
        # The current structure will re-raise to the generic `except Exception` if this block doesn't return.
        # To ensure retry for this specific error type, we can explicitly call the retry logic or re-raise
        # No, the current structure, if this doesn't return, falls through to the general exception. So it's fine.
        # For clarity:
        # raise # This would re-raise the RuntimeError to be caught by the generic Exception handler.
        return {"status": "error", "message": str(rte)} # If we don't want to retry this specific one.
                                                        # Let's assume we want all errors to potentially retry.

    except Exception as e:
        active_logger.error(f"Critical error in analyze_website_schema: {e}", exc_info=True)
        # Attempt to publish to a retry topic if configured and relevant info is available
        if publisher_client and gcp_project_id: # Ensure publisher and project_id are initialized
            retry_topic_name_from_config = project_config.get("topics",{}).get("pipeline_retry", RETRY_TOPIC_NAME)
            if retry_topic_name_from_config:
                try:
                    current_retry_count = message_payload.get("retry_info", {}).get("count", 0)
                    max_retries = project_config.get("max_pipeline_retries", 3)

                    if current_retry_count < max_retries:
                        retry_payload_out = {
                            "original_message": message_payload, # Include original payload for context
                            "error_info": {
                                "message": sanitize_error_message(str(e)),
                                "stage": "analyze_website_schema",
                                "timestamp_utc": datetime.utcnow().isoformat() + "Z"
                            },
                            "retry_info": {
                                "count": current_retry_count + 1,
                                "max_retries": max_retries,
                                "next_attempt_delay_seconds": RETRY_BACKOFF * (2 ** current_retry_count) # Informative
                            }
                        }
                        retry_topic_path = publisher_client.topic_path(gcp_project_id, retry_topic_name_from_config)
                        publish_future = publisher_client.publish(retry_topic_path, json.dumps(retry_payload_out).encode("utf-8"))
                        publish_future.result(timeout=10) # Wait for publish, with a timeout
                        active_logger.info(f"Published error to retry topic '{retry_topic_name_from_config}' for attempt {current_retry_count + 1}/{max_retries}.")
                    else:
                        active_logger.error(f"Max retries ({max_retries}) reached for message originating from {csv_gcs_path}. No further retries will be attempted for this error.")
                except Exception as retry_pub_error:
                    active_logger.error(f"Failed to publish to retry topic '{retry_topic_name_from_config}': {retry_pub_error}", exc_info=True)
            else:
                active_logger.warning("Retry topic not configured. Cannot publish for retry.")
        else:
            active_logger.warning("Publisher client or GCP Project ID not available. Cannot publish to retry topic.")
        
        # Depending on desired behavior, re-raise or return error status
        # Re-raising makes it clearer in Cloud Functions logs that it's an unhandled error.
        # However, for Pub/Sub triggers, Cloud Functions will auto-retry on unhandled exceptions.
        # If we have custom retry logic, returning a success to Pub/Sub might be intended if the retry message was sent.
        # For now, let's return an error status without re-raising to avoid double retry if Pub/Sub retries too.
        return {"status": "error", "message": f"Critical error: {sanitize_error_message(str(e))}"}