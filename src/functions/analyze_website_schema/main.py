# src/functions/analyze_website_schema/main.py

import json
import base64
import logging
import os
import random
from urllib.parse import urljoin
import re

import requests
from bs4 import BeautifulSoup
from google.cloud import firestore, pubsub_v1, storage
import vertexai
from vertexai.generative_models import GenerativeModel, Part, GenerationConfig
import functions_framework
import pandas as pd
from io import StringIO

# Import common modules from your pipeline
from src.common.utils import setup_logging
from src.common.config import load_customer_config, load_project_config
from src.common.helpers import validate_html_content, sanitize_error_message

# Initialize logger
logger = logging.getLogger(__name__)

# Constants
NEXT_STEP_INGEST_CATEGORY_URLS_TOPIC_NAME = "start-ingest-category-urls-topic"
MAX_HTML_FOR_PROMPT = 3000000  # Max characters for LLM prompt
SAMPLE_SIZE = 10  # Number of URLs to sample for analysis
REQUEST_TIMEOUT = 30  # Seconds for HTTP requests
MAX_FIRESTORE_RETRIES = 3
RETRY_BACKOFF = 2  # Seconds
RETRY_TOPIC_NAME = "retry-pipeline"

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
                "include_metadata": True,
                "expand_accordions": True,
                "trigger_lazy_loading": True,
                "timeout": timeout * 1000,  # Convert seconds to milliseconds
                "max_wait": 10  # Wait up to 10 seconds for dynamic content
            }

            # Call web-renderer service
            response = requests.post(
                renderer_url,
                json=payload,
                timeout=timeout,
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()

            # Parse response
            result = response.json()
            if "html" not in result:
                active_logger.error(f"No HTML returned from web-renderer for {url}: {result}")
                return None

            html_content = result.get("html")
            if not validate_html_content(html_content, active_logger):
                return None

            active_logger.info(f"Fetched rendered HTML for {url} via web-renderer, interactions: {result.get('interactions', [])}")
            return html_content

        except Exception as e:
            active_logger.warning(f"Web-renderer failed for {url}: {e}. Falling back to static fetch.")
    
    # Fallback to static fetch
    try:
        response = requests.get(
            url,
            timeout=timeout,
            headers={'User-Agent': 'GoogleCloudFunction-AnalyzeWebsiteSchema/1.0'}
        )
        response.raise_for_status()
        
        if 'text/html' not in response.headers.get('Content-Type', '').lower():
            active_logger.warning(f"Non-HTML content at {url}: {response.headers.get('Content-Type')}")
            return None
            
        html_content = validate_html_content(response.content, active_logger)
        active_logger.info(f"Static fetch succeeded for {url}")
        return html_content
        
    except requests.RequestException as e:
        active_logger.error(f"Static fetch also failed for {url}: {e}")
        return None


def extract_main_urls_from_category_page(html_content, base_url, logger_instance=None):
    """Enhanced URL extraction with better heuristics for different site types."""
    active_logger = logger_instance or logger
    if not html_content:
        return []
    
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        main_urls = set()
        
        # Enhanced selectors for different types of sites
        selectors = [
            # Standard content selectors
            'article a', 'div.item a', 'h2 a', 'h3 a', 'li a',
            
            # Government/regulatory site patterns
            'a[href*="/report"]', 'a[href*="/guide"]', 'a[href*="/consultation"]',
            'a[href*="/media-release"]', 'a[href*="/document"]', 'a[href*="/instrument"]',
            'a[href*="/law"]', 'a[href*="/article"]', 'a[href*="/publication"]',
            
            # Table-based content (common in government sites)
            'table td a', 'table th a',
            
            # List-based navigation
            'ul.content-list a', 'ol.content-list a', 'div.content-list a',
            
            # Common CMS patterns
            '.post-title a', '.entry-title a', '.content-title a',
            '.document-link', '.resource-link',
            
            # ASIC-specific patterns
            'a[href*="/rep-"]', 'a[href*="/rg-"]', 'a[href*="/cp-"]', 'a[href*="/mr-"]'
        ]
        
        for selector in selectors:
            try:
                for link in soup.select(selector):
                    if link.get('href'):
                        href = link['href'].strip()
                        link_text = link.get_text(strip=True)
                        
                        # Skip navigation and utility links
                        if (href and 
                            not href.startswith(('#', 'javascript:', 'mailto:', 'tel:')) and
                            href != '/' and
                            len(link_text) > 2 and  # Meaningful link text
                            not any(skip in link_text.lower() for skip in ['home', 'contact', 'about', 'privacy', 'terms'])):
                            
                            absolute_url = urljoin(base_url, href)
                            main_urls.add(absolute_url)
            except Exception as e:
                active_logger.debug(f"Error with selector '{selector}': {e}")
                continue
        
        # Also look for PDF links as potential main URLs
        pdf_links = soup.find_all('a', href=lambda x: x and x.lower().endswith('.pdf'))
        for pdf_link in pdf_links:
            href = pdf_link.get('href')
            if href:
                absolute_url = urljoin(base_url, href)
                main_urls.add(absolute_url)
        
        active_logger.info(f"Extracted {len(main_urls)} potential main URLs from {base_url}")
        return list(main_urls)
        
    except Exception as e:
        active_logger.error(f"Error extracting main URLs from {base_url}: {e}")
        return []


def analyze_html_with_ai(html_samples, project_config, gcp_project_id, logger_instance=None):
    """Enhanced AI analysis with updated models and better prompting."""
    active_logger = logger_instance or logger
    ai_config = project_config.get("ai_model_config", {})
    model_id = ai_config.get("schema_analysis_model_id", "gemini-2.0-flash-001")  # Updated default
    vertex_ai_location = project_config.get("vertex_ai_location", "europe-west1")

    if not html_samples:
        active_logger.warning("No HTML samples provided for AI analysis.")
        return None

    try:
        vertexai.init(project=gcp_project_id, location=vertex_ai_location)
        model = GenerativeModel(model_id)

        # Truncate HTML samples to fit within context window
        truncated_samples = []
        max_html_per_sample = MAX_HTML_FOR_PROMPT // max(len(html_samples), 1)
        
        for sample in html_samples:
            truncated_html = sample["html"][:max_html_per_sample]
            if len(sample["html"]) > max_html_per_sample:
                active_logger.warning(f"Truncated HTML for {sample['url']} from {len(sample['html'])} to {len(truncated_html)} chars.")
            truncated_samples.append({
                "url": sample["url"], 
                "html": truncated_html, 
                "type": sample["type"]
            })

        # Enhanced prompt for better schema analysis
        samples_json = json.dumps(truncated_samples, indent=2)
        prompt = f"""
        Analyze the following HTML samples from a website to propose a comprehensive metadata schema, field mappings, XML structure, and configuration.

        HTML Samples:
        ```json
        {samples_json}
        ```

        Your task is to create a complete configuration for processing this website. Analyze the HTML structure, content patterns, and identify:

        1. **metadata_fields**: List of metadata fields that can be extracted from the documents.
           Include field name, description, and likely CSS selector or extraction pattern.
           Example: [{{"name": "title", "description": "Document title", "selector": "h1.main-title"}}]

        2. **field_mappings**: Mappings for each metadata field to its source in HTML or computed value.
           Example: {{"title": {{"source": "h1.main-title", "type": "direct"}}, "document_id": {{"source": "generate_from_url", "type": "computed"}}}}

        3. **xml_structure**: Proposed hierarchical XML structure for the metadata and content.
           Example:
           {{
             "root_tag": "Document",
             "filename_template": "{{indexed_filename_base}}.xml",
             "fields": [
               {{"tag": "DocumentID", "source": "document_id", "cdata": false}},
               {{"tag": "Title", "source": "title", "cdata": false}},
               {{"tag": "Content", "source": "gcs_html_content", "cdata": true}}
             ]
           }}

        4. **language_and_country**: Detect the primary language and country of the website.
           Example: {{"language": "English", "language_code": "en", "country": "Australia"}}

        5. **discovery_selectors**: CSS selectors for discovering main URLs and pagination on category pages.
           Example: {{
             "main_url_selectors": ["article a.title-link", "h2 a", "table td a"],
             "pagination_selector": "a.next-page"
           }}

        6. **heuristic_link_extraction**: Configuration for heuristic-based link discovery.
           Example: {{
             "enabled": true,
             "content_selectors": ["main", "article", ".content"],
             "exclusion_selectors": ["nav", "footer", ".sidebar"],
             "link_text_keywords": ["report", "guide", "document"],
             "href_patterns": ["\\\\.pdf$", "/reports/", "/documents/"],
             "min_link_text_len": 5
           }}

        7. **document_type_patterns**: Patterns to classify different document types.
           Example: {{
             "Media Release": ["media release", "mr-", "announcement"],
             "Report": ["report", "rep-", "analysis"],
             "Guide": ["guide", "rg-", "regulatory guide"]
           }}

        Return a single, valid JSON object containing ALL the above sections. Focus on practical, working selectors and patterns based on the actual HTML structure you observe.

        If this appears to be a government/regulatory website (like ASIC), include appropriate patterns for regulatory documents, reports, media releases, and consultation papers.

        Ensure all field names use underscore_case and all selectors are valid CSS.
        """
        
        generation_config = GenerationConfig(
            temperature=0.2,  # Lower temperature for more consistent results
            top_p=0.8,
            top_k=20,
            max_output_tokens=8192,  # Increased for comprehensive response
            response_mime_type="application/json"
        )

        active_logger.info(f"Sending request to Vertex AI (model: {model_id}) for website schema analysis.")
        
        response = model.generate_content(
            [Part.from_text(prompt)],
            generation_config=generation_config
        )

        if not response.candidates or not response.candidates[0].content.parts:
            active_logger.error("No valid response from AI model")
            return None

        response_text = response.candidates[0].content.parts[0].text.strip()
        active_logger.info(f"AI analysis raw response: {response_text[:500]}...")
        
        try:
            ai_derived_config = json.loads(response_text)
            
            # Validate required keys
            required_keys = [
                "metadata_fields", "field_mappings", "xml_structure",
                "language_and_country", "discovery_selectors", 
                "heuristic_link_extraction", "document_type_patterns"
            ]
            
            missing_keys = [key for key in required_keys if key not in ai_derived_config]
            if missing_keys:
                active_logger.warning(f"AI response missing keys: {missing_keys}. Adding defaults.")
                
                # Add default values for missing keys
                if "discovery_selectors" not in ai_derived_config:
                    ai_derived_config["discovery_selectors"] = {
                        "main_url_selectors": ["article a", "h2 a", "h3 a"],
                        "pagination_selector": "a.next"
                    }
                
                if "heuristic_link_extraction" not in ai_derived_config:
                    ai_derived_config["heuristic_link_extraction"] = {
                        "enabled": True,
                        "content_selectors": ["main", "article", ".content"],
                        "exclusion_selectors": ["nav", "footer"],
                        "link_text_keywords": ["document", "report"],
                        "href_patterns": ["\\.pdf$"],
                        "min_link_text_len": 5
                    }
                
                if "document_type_patterns" not in ai_derived_config:
                    ai_derived_config["document_type_patterns"] = {
                        "Report": ["report"],
                        "Document": ["document"]
                    }
            
            active_logger.info(f"Successfully parsed AI-derived schema config with {len(ai_derived_config)} sections.")
            return ai_derived_config
            
        except json.JSONDecodeError as e:
            active_logger.error(f"Failed to decode AI JSON response: {e}. Response: {response_text}")
            return None

    except Exception as e:
        active_logger.error(f"Vertex AI analysis failed: {e}", exc_info=True)
        return None


@functions_framework.cloud_event
def analyze_website_schema(cloud_event):
    """
    Enhanced website schema analysis with consistent web-renderer usage and improved AI prompting.
    Analyzes a sample of category and main URLs to propose a metadata schema using Vertex AI.
    """
    active_logger = logger
    publisher_client = None
    gcp_project_id = os.environ.get("GCP_PROJECT")

    try:
        pubsub_message_data = base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8")
        message_payload = json.loads(pubsub_message_data)
        message_id = cloud_event.data["message"].get("messageId", "N/A")

        customer = message_payload.get("customer")
        project_config_name = message_payload.get("project")
        csv_gcs_path = message_payload.get("csv_gcs_path")

        if not all([customer, project_config_name, csv_gcs_path]):
            active_logger.error(f"Missing required fields in message {message_id}: customer, project, or csv_gcs_path.")
            raise ValueError("Missing required Pub/Sub fields.")

        active_logger = setup_logging(customer, project_config_name)
        active_logger.info(f"Processing message {message_id} for schema analysis. Customer: {customer}, Project: {project_config_name}, CSV: {csv_gcs_path}")

        # Load configurations
        customer_config = load_customer_config(customer)
        project_config = load_project_config(project_config_name)
        gcp_project_id_customer = customer_config.get("gcp_project_id")
        if gcp_project_id_customer:
            gcp_project_id = gcp_project_id_customer

        if not gcp_project_id:
            active_logger.error("GCP Project ID not configured.")
            raise ValueError("GCP Project ID not configured.")

        # Initialize clients
        storage_client = storage.Client(project=gcp_project_id)
        bucket_name = project_config.get("gcs_bucket")
        if not csv_gcs_path.startswith(f"gs://{bucket_name}/"):
            active_logger.error(f"CSV GCS path {csv_gcs_path} does not match configured bucket {bucket_name}.")
            raise ValueError("Invalid CSV GCS path.")

        bucket = storage_client.bucket(bucket_name)
        blob_name = csv_gcs_path.replace(f"gs://{bucket_name}/", "")
        blob = bucket.blob(blob_name)

        if not blob.exists():
            active_logger.error(f"CSV file not found at {csv_gcs_path}.")
            raise FileNotFoundError(f"CSV file not found at {csv_gcs_path}.")

        db = firestore.Client(project=gcp_project_id, database=project_config.get("firestore_database_id", "(default)"))
        publisher_client = pubsub_v1.PublisherClient()

        # Read CSV and sample category URLs
        csv_content = blob.download_as_text()
        csv_file = StringIO(csv_content)
        df = pd.read_csv(csv_file)
        if "url" not in df.columns:
            active_logger.error("CSV does not contain a 'url' column.")
            raise ValueError("CSV missing 'url' column.")

        category_urls = df["url"].dropna().tolist()
        if not category_urls:
            active_logger.error("No valid URLs found in CSV.")
            raise ValueError("No valid URLs in CSV.")

        # Sample URLs for analysis
        sample_size = min(SAMPLE_SIZE, len(category_urls))
        sample_category_urls = random.sample(category_urls, sample_size)
        active_logger.info(f"Sampled {len(sample_category_urls)} category URLs for analysis.")

        # Fetch HTML and extract main URLs
        html_samples = []
        total_samples_target = 20  # Target total samples (category + main)
        
        for url in sample_category_urls:
            # Fetch category page
            html_content = fetch_page_content(url, project_config, logger_instance=active_logger)
            if html_content:
                html_samples.append({"url": url, "html": html_content, "type": "category"})
                active_logger.info(f"Added category page sample: {url}")
                
                # Extract and sample main URLs from this category page
                main_urls = extract_main_urls_from_category_page(html_content, url, active_logger)
                if main_urls:
                    # Sample 2-3 main URLs per category page
                    num_main_samples = min(3, len(main_urls), max(1, (total_samples_target - len(html_samples)) // len(sample_category_urls)))
                    sampled_main_urls = random.sample(main_urls, num_main_samples)
                    
                    for main_url in sampled_main_urls:
                        main_html = fetch_page_content(main_url, project_config, logger_instance=active_logger)
                        if main_html:
                            html_samples.append({"url": main_url, "html": main_html, "type": "main"})
                            active_logger.info(f"Added main page sample: {main_url}")
                            
                            # Stop if we have enough samples
                            if len(html_samples) >= total_samples_target:
                                break
                
                # Stop if we have enough samples
                if len(html_samples) >= total_samples_target:
                    break

        active_logger.info(f"Collected {len(html_samples)} HTML samples ({sum(1 for s in html_samples if s['type'] == 'category')} category, {sum(1 for s in html_samples if s['type'] == 'main')} main URLs).")

        if len(html_samples) < 2:
            active_logger.error("Insufficient HTML samples collected for analysis.")
            raise RuntimeError("Could not collect enough HTML samples for schema analysis.")

        # Analyze with Vertex AI
        ai_schema = analyze_html_with_ai(html_samples, project_config, gcp_project_id, active_logger)
        if not ai_schema:
            active_logger.error("AI schema analysis failed to produce a valid schema.")
            raise RuntimeError("AI schema analysis failed.")

        # Store schema in Firestore with merge to preserve existing project config
        project_doc_ref = db.collection("projects").document(project_config_name)
        for attempt in range(MAX_FIRESTORE_RETRIES):
            try:
                project_doc_ref.set({
                    "metadata_schema": ai_schema,
                    "schema_generated_timestamp": firestore.SERVER_TIMESTAMP,
                    "csv_gcs_path": csv_gcs_path,
                    "schema_analysis_samples_count": len(html_samples)
                }, merge=True)
                active_logger.info(f"Stored schema in Firestore for project {project_config_name}.")
                break
            except Exception as e:
                active_logger.warning(f"Firestore set attempt {attempt + 1} failed: {e}")
                if attempt < MAX_FIRESTORE_RETRIES - 1:
                    import time
                    time.sleep(RETRY_BACKOFF * (2 ** attempt))
                else:
                    active_logger.error(f"Max retries reached for Firestore set: {e}")
                    raise

        # Store schema in GCS for reference and backup
        schema_gcs_path = f"schemas/{project_config_name}/schema_{message_id}.json"
        schema_blob = bucket.blob(schema_gcs_path)
        schema_data = {
            "schema": ai_schema,
            "metadata": {
                "generated_timestamp": datetime.now().isoformat(),
                "samples_analyzed": len(html_samples),
                "category_urls_sampled": len(sample_category_urls),
                "ai_model": ai_config.get("schema_analysis_model_id", "gemini-2.0-flash-001"),
                "csv_source": csv_gcs_path
            }
        }
        schema_blob.upload_from_string(
            json.dumps(schema_data, indent=2), 
            content_type="application/json"
        )
        active_logger.info(f"Stored schema backup at gs://{bucket_name}/{schema_gcs_path}")

        # Trigger next step (discover_main_urls via ingest_category_urls)
        next_step_payload = {
            "customer": customer,
            "project": project_config_name,
            "csv_gcs_path": csv_gcs_path
        }
        topic_path = publisher_client.topic_path(gcp_project_id, NEXT_STEP_INGEST_CATEGORY_URLS_TOPIC_NAME)
        future = publisher_client.publish(topic_path, json.dumps(next_step_payload).encode("utf-8"))
        future.result()
        active_logger.info(f"Published to {NEXT_STEP_INGEST_CATEGORY_URLS_TOPIC_NAME} for {project_config_name}")

        return {
            "status": "success",
            "project": project_config_name,
            "schema_gcs_path": f"gs://{bucket_name}/{schema_gcs_path}",
            "samples_analyzed": len(html_samples)
        }

    except ValueError as ve:
        active_logger.error(f"Configuration or input error: {ve}", exc_info=True)
        return {"status": "error", "message": str(ve)}
    except Exception as e:
        active_logger.error(f"Critical error in analyze_website_schema: {e}", exc_info=True)
        if publisher_client and 'gcp_project_id' in locals() and gcp_project_id:
            try:
                retry_payload = {
                    "customer": customer if 'customer' in locals() else "unknown",
                    "project": project_config_name if 'project_config_name' in locals() else "unknown",
                    "csv_gcs_path": csv_gcs_path if 'csv_gcs_path' in locals() else "unknown",
                    "error_message": sanitize_error_message(str(e)),
                    "stage": "analyze_website_schema",
                    "retry_count": message_payload.get("retry_count", 0) + 1 if 'message_payload' in locals() else 1
                }
                retry_topic_path = publisher_client.topic_path(gcp_project_id, RETRY_TOPIC_NAME)
                publisher_client.publish(retry_topic_path, json.dumps(retry_payload).encode("utf-8"))
                active_logger.info(f"Published error to {RETRY_TOPIC_NAME} for retry.")
            except Exception as retry_error:
                active_logger.error(f"Failed to publish to retry topic: {retry_error}")
        raise