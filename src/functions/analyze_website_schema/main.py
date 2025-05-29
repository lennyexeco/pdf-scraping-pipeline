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
from vertexai.language_models import TextGenerationModel
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

def fetch_page_content(url, timeout=REQUEST_TIMEOUT, logger_instance=None):
    """Fetches HTML content from a URL using web-renderer service for dynamic content."""
    active_logger = logger_instance or logger
    # Get web-renderer URL from environment or project config
    project_config = load_project_config(project_id)
    renderer_url = project_config.get('web_renderer_url', None)

    try:
        # Prepare payload for web-renderer
        payload = {
            "url": url,
            "include_metadata": True,  # Get interaction details for debugging
            "expand_accordions": True,  # Expand accordions
            "activate_tabs": False,  # Optional, adjust based on site
            "trigger_lazy_loading": True,  # Handle lazy-loaded content
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
            active_logger.error(f"No HTML returned for {url}: {result}")
            return None

        html_content = result.get("html")
        if not validate_html_content(html_content, active_logger):
            return None

        active_logger.info(f"Fetched rendered HTML for {url}, interactions: {result.get('interactions', [])}")
        return html_content

    except requests.RequestException as e:
        active_logger.error(f"Failed to fetch URL {url}: {url} via web-renderer: {e}")
        # Fallback to static fetch if web-renderer fails (optional)
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
            active_logger.info(f"Fallback to static fetch succeeded for {url}")
            return html_content
        except requests.RequestException as e2:
            active_logger.error(f"Fallback fetch failed for {url}: {e2}")
            return None

def extract_main_urls_from_category_page(html_content, base_url, logger_instance=None):
    """Extracts main URLs from a category page using simple heuristics."""
    active_logger = logger_instance or logger
    if not html_content:
        return []
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        main_urls = set()
        # Common selectors for main URLs (to be refined by LLM later)
        selectors = [
            'article a', 'div.item a', 'h2 a', 'h3 a', 'li a', 'a[href*="/law"]',
            'a[href*="/article"]', 'a[href*="/document"]'
        ]
        for selector in selectors:
            for link in soup.select(selector):
                if link.get('href'):
                    href = link['href'].strip()
                    if href and not href.startswith(('#', 'javascript:', 'mailto:')):
                        absolute_url = urljoin(base_url, href)
                        main_urls.add(absolute_url)
        active_logger.info(f"Extracted {len(main_urls)} potential main URLs from {base_url}")
        return list(main_urls)
    except Exception as e:
        active_logger.error(f"Error extracting main URLs from {base_url}: {e}")
        return []

def analyze_html_with_ai(html_samples, project_config, gcp_project_id, logger_instance=None):
    """Uses Vertex AI to analyze HTML samples and propose a metadata schema."""
    active_logger = logger_instance or logger
    ai_config = project_config.get("ai_model_config", {})
    model_id = ai_config.get("schema_analysis_model_id", "text-bison@001")
    vertex_ai_location = project_config.get("vertex_ai_location", "europe-west1")

    if not html_samples:
        active_logger.warning("No HTML samples provided for AI analysis.")
        return None

    try:
        vertexai.init(project=gcp_project_id, location=vertex_ai_location)
        model = TextGenerationModel.from_pretrained(model_id)

        # Truncate HTML samples to fit within context window
        truncated_samples = []
        for sample in html_samples:
            truncated_html = sample["html"][:MAX_HTML_FOR_PROMPT // len(html_samples)]
            if len(sample["html"]) > MAX_HTML_FOR_PROMPT // len(html_samples):
                active_logger.warning(f"Truncated HTML for {sample['url']} from {len(sample['html'])} to {len(truncated_html)} chars.")
            truncated_samples.append({"url": sample["url"], "html": truncated_html, "type": sample["type"]})

        # Construct prompt
        samples_json = json.dumps(truncated_samples, indent=2)
        prompt = (
            "Analyze the following HTML samples from a law firm website to propose a metadata schema, field mappings, XML structure, and language/country detection.\n\n"
            "HTML Samples:\n"
            "```json\n"
            f"{samples_json}\n"
            "```\n\n"
            "Your task is to:\n\n"
            "1. `metadata_fields`: List of metadata fields to extract (e.g., title, issue_date, law_id, abbreviation).\n"
            "   Include field name, description, and likely CSS selector or extraction pattern.\n"
            '   Example: [{"name": "title", "description": "Document title", "selector": "h1.jntitel"}]\n\n'
            "2. `field_mappings`: Mappings for each metadata field to its source in HTML or computed value.\n"
            '   Example: {"title": {"source": "h1.jntitel", "type": "direct"}, "law_id": {"source": "generate_law_id", "type": "computed"}}\n\n'
            "3. `xml_structure`: Proposed hierarchical XML structure for the metadata and content.\n"
            "   Example:\n"
            "   {\n"
            '     "root_tag": "LegalDocument",\n'
            '     "fields": [\n'
            '       {"tag": "Metadata", "subfields": [{"tag": "Gesetzesausweis", "source": "law_id"}]},'
            '       {"tag": "Content", "source": "gcs_html_content", "cdata": true}'
            "     ]\n"
            "   }\n\n"
            "4. `language_and_country`: Detect the primary language and country of the website.\n"
            '   Example: {"language": "German", "country": "Germany"}\n\n'
            "5. `tag_mapping`: Mapping of English field names to localized XML tags based on detected language.\n"
            '   Example: {"law_id": "Gesetzesausweis", "issue_date": "Ausfertigungsdatum"}\n\n'
            "6. `fallback_selectors`: Fallback CSS selectors for main URLs and pagination on category pages.\n"
            '   Example: {"main_url_selectors": ["article a"], "pagination_selector": "a.next-page"}\n\n'
            "Return a single JSON object containing all the above. Example:\n"
            "```json\n"
            "{\n"
            '  "metadata_fields": [...],\n'
            '  "field_mappings": {...},\n'
            '  "xml_structure": {...},\n'
            '  "language_and_country": {...},\n'
            '  "tag_mapping": {...},\n'
            '  "fallback_selectors": {...}\n'
            "}\n"
            "```\n\n"
            "Ensure field names and tags are in the detected language where applicable.\n"
            "If no clear selectors or fields are found, provide reasonable defaults or empty values."
        )

        active_logger.info(f"Sending request to Vertex AI (model: {model_id}) for website schema analysis.")
        response = model.predict(
            prompt,
            temperature=0.3,  # Moderate creativity
            top_p=0.8,
            top_k=20,
            max_output_tokens=4096
        )

        response_text = response.text.strip()
        active_logger.info(f"AI analysis raw response: {response_text[:500]}...")
        try:
            # Ensure response is valid JSON
            if not response_text.startswith('{'):
                response_text = response_text.strip('```json\n').rstrip('```')
            ai_derived_config = json.loads(response_text)
            required_keys = ["metadata_fields", "field_mappings", "xml_structure",
                            "language_and_country", "tag_mapping", "fallback_selectors"]
            if all(key in ai_derived_config for key in required_keys):
                active_logger.info(f"Successfully parsed AI-derived schema config.")
                return ai_derived_config
            else:
                active_logger.error(f"AI response missing required keys: {response_text}")
                return None
        except json.JSONDecodeError as e:
            active_logger.error(f"Failed to decode AI JSON response: {e}. Response: {response_text}")
            return None

    except Exception as e:
        active_logger.error(f"Vertex AI analysis failed: {e}", exc_info=True)
        return None

@functions_framework.cloud_event
def analyze_website_schema(cloud_event):
    """
    Analyzes a sample of category and main URLs to propose a metadata schema using Vertex AI.
    Triggered by Pub/Sub with customer, project, and CSV GCS path.

    Args:
        cloud_event: Pub/Sub event with message data.

    Returns:
        Dict with status and schema GCS path.
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
        csv_gcs_path = message_payload.get("csv_gcs_path")  # e.g., gs://bucket/path/to/category_urls.csv

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

        sample_category_urls = random.sample(category_urls, min(SAMPLE_SIZE, len(category_urls)))
        active_logger.info(f"Sampled {len(sample_category_urls)} category URLs for analysis.")

        # Fetch HTML and extract main URLs
        html_samples = []
        for url in sample_category_urls:
            html_content = fetch_page_content(url, logger_instance=active_logger)
            if html_content:
                html_samples.append({"url": url, "html": html_content, "type": "category"})
                # Extract main URLs from category page
                main_urls = extract_main_urls_from_category_page(html_content, url, active_logger)
                sampled_main_urls = random.sample(main_urls, min(2, len(main_urls))) if main_urls else []
                for main_url in sampled_main_urls:
                    main_html = fetch_page_content(main_url, logger_instance=active_logger)
                    if main_html:
                        html_samples.append({"url": main_url, "html": main_html, "type": "main"})

        active_logger.info(f"Collected {len(html_samples)} HTML samples (category and main URLs).")

        # Analyze with Vertex AI
        ai_schema = analyze_html_with_ai(html_samples, project_config, gcp_project_id, active_logger)
        if not ai_schema:
            active_logger.error("AI schema analysis failed to produce a valid schema.")
            raise RuntimeError("AI schema analysis failed.")

        # Store schema in Firestore
        project_doc_ref = db.collection("projects").document(project_config_name)
        for attempt in range(MAX_FIRESTORE_RETRIES):
            try:
                project_doc_ref.set({
                    "metadata_schema": ai_schema,
                    "schema_generated_timestamp": firestore.SERVER_TIMESTAMP,
                    "csv_gcs_path": csv_gcs_path
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

        # Store schema in GCS for reference
        schema_gcs_path = f"schemas/{project_config_name}/schema_{message_id}.json"
        schema_blob = bucket.blob(schema_gcs_path)
        schema_blob.upload_from_string(json.dumps(ai_schema, indent=2), content_type="application/json")
        active_logger.info(f"Stored schema at gs://{bucket_name}/{schema_gcs_path}")

        # Trigger next step (ingest_category_urls)
        next_step_payload = {
            "customer": customer,
            "project": project_config_name,
            "csv_gcs_path": csv_gcs_path
        }
        topic_path = publisher_client.topic_path(gcp_project_id, NEXT_STEP_INGEST_CATEGORY_URLS_TOPIC_NAME)
        publisher_client.publish(topic_path, json.dumps(next_step_payload).encode("utf-8"))
        active_logger.info(f"Published to {NEXT_STEP_INGEST_CATEGORY_URLS_TOPIC_NAME} for {project_config_name}")

        return {
            "status": "success",
            "project": project_config_name,
            "schema_gcs_path": f"gs://{bucket_name}/{schema_gcs_path}"
        }

    except ValueError as ve:
        active_logger.error(f"Configuration or input error: {ve}", exc_info=True)
        return {"status": "error", "message": str(ve)}
    except Exception as e:
        active_logger.error(f"Critical error in analyze_website_schema: {e}", exc_info=True)
        if publisher_client:
            retry_payload = {
                "customer": customer if 'customer' in locals() else "unknown",
                "project": project_config_name if 'project_config_name' in locals() else "unknown",
                "csv_gcs_path": csv_gcs_path if 'csv_gcs_path' in locals() else "unknown",
                "error_message": sanitize_error_message(str(e)),
                "stage": "analyze_website_schema",
                "retry_count": message_payload.get("retry_count", 0) + 1 if 'message_payload' in locals() else 1
            }
            publisher_client.publish(
                publisher_client.topic_path(gcp_project_id, RETRY_TOPIC_NAME),
                json.dumps(retry_payload).encode("utf-8")
            )
            active_logger.info(f"Published error to {RETRY_TOPIC_NAME} for retry.")
        raise