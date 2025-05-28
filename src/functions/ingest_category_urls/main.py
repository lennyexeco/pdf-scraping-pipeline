import json
import base64
import logging
import os
import csv
from io import StringIO
from datetime import datetime # Added import that was missing in your snippet but used

from google.cloud import pubsub_v1, storage
import functions_framework

# Assuming your common modules are structured as described
# You might need to adjust PYTHONPATH for local testing if 'src' is not in it.
# For Cloud Functions deployment, if you deploy from the root of your project
# and your 'src' directory is included, this import style should work.
from src.common.utils import setup_logging
from src.common.config import load_customer_config, load_project_config

# Initialize a basic logger. It will be reconfigured by setup_logging later.
logger = logging.getLogger(__name__)

# Environment variables or configuration
DISCOVER_MAIN_URLS_TOPIC_NAME_ENV = "DISCOVER_MAIN_URLS_TOPIC" # e.g., discover-main-urls-topic
# It's good practice to define this, or fetch from project_config if it varies per project
# DISCOVER_MAIN_URLS_TOPIC_NAME_DEFAULT = "discover-main-urls-topic"

@functions_framework.cloud_event
def ingest_category_urls(cloud_event):
    """
    Triggered by a GCS event when a CSV file is uploaded.
    Expected GCS event payload structure contains 'bucket' and 'name' (file path).
    """
    event_data = cloud_event.data
    bucket_name = event_data.get("bucket")
    file_name = event_data.get("name") # e.g., <project_name>/inputs/category_urls.csv

    if not bucket_name or not file_name:
        # Use the module-level logger if setup_logging hasn't happened yet
        logger.error("GCS event missing bucket name or file name.")
        raise ValueError("Invalid GCS event data: missing bucket or file name.")

    # Initial log before specific logger is set up
    logger.info(f"GCS event triggered for file: gs://{bucket_name}/{file_name}")

    # Extract customer and project from the file path.
    # This convention is crucial for the pipeline.
    path_parts = file_name.split('/')
    if len(path_parts) < 2: # Expecting at least <project_name_for_config>/path/to/file.csv
        logger.error(f"Could not determine project_config_name from GCS path: {file_name}. Expected <project_config_name>/... format.")
        raise ValueError(f"Invalid GCS path format: {file_name}. Expected <project_config_name>/...")

    # project_config_name is used to load project-specific configurations (e.g., CSV column index)
    # and will be passed along for downstream functions to load their dynamic site configs.
    project_config_name = path_parts[0]
    
    # Customer might be fixed, from env, or also from path if GCS structure is more complex
    # e.g., gs://<bucket>/<customer_id>/<project_config_name>/inputs/...
    customer_id = os.environ.get("CUSTOMER_ID", "harvey") # Defaulting to "harvey" as in your code

    active_logger = setup_logging(customer_id, project_config_name)
    active_logger.info(f"Processing GCS trigger for customer '{customer_id}', project_config_name '{project_config_name}'. File: gs://{bucket_name}/{file_name}")

    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        
        if not blob.exists():
            active_logger.error(f"File not found in GCS: gs://{bucket_name}/{file_name}")
            # No need to raise FileNotFoundError again, error is logged, and CF will terminate.
            # Or, if you want a specific HTTP error for GCS trigger, this is fine.
            raise FileNotFoundError(f"File gs://{bucket_name}/{file_name} not found.")

        csv_content_str = blob.download_as_text()
        # Pass project_config_name to _process_csv_content
        _process_csv_content(csv_content_str, customer_id, project_config_name, active_logger)
        active_logger.info(f"Successfully processed category URLs from gs://{bucket_name}/{file_name}")
        return {"status": "success", "message": f"Processed {file_name}"}

    except FileNotFoundError:
        # Already logged by the part that raises it. Re-raising marks function failure.
        raise
    except Exception as e:
        active_logger.error(f"Error processing GCS event for file gs://{bucket_name}/{file_name}: {str(e)}", exc_info=True)
        # Depending on the error, you might want to publish to a dead-letter/retry topic
        raise # Re-raise to mark function failure

@functions_framework.http
def ingest_category_urls_http(request):
    """
    Triggered by an HTTP POST request.
    Expected JSON payload:
    {
        "customer": "customer_id",
        "project": "project_config_name", // This is the project_config_name for loading configs
        "csv_gcs_path": "gs://bucket_name/path/to/category_urls.csv" (optional)
        "csv_content": "url1\\nurl2\\nurl3" (optional, if not using csv_gcs_path)
    }
    """
    # Ensure active_logger is defined in broader scope for the final except block
    active_logger = logger # Default to module logger

    if request.method != "POST":
        return ("Only POST requests are accepted", 405)

    try:
        request_json = request.get_json(silent=True)
        if not request_json:
            return ("Invalid JSON payload", 400)

        customer_id = request_json.get("customer")
        project_config_name = request_json.get("project") # This should map to your project config file name
        csv_gcs_path = request_json.get("csv_gcs_path") 
        csv_content_str_direct = request_json.get("csv_content")

        if not customer_id or not project_config_name:
            return ("Missing 'customer' or 'project' (for project_config_name) in JSON payload", 400)
        
        if not csv_gcs_path and not csv_content_str_direct:
            return ("Either 'csv_gcs_path' or 'csv_content' must be provided in JSON payload", 400)

        # Initialize customer/project specific logger
        active_logger = setup_logging(customer_id, project_config_name)
        active_logger.info(f"Processing HTTP trigger for customer '{customer_id}', project_config_name '{project_config_name}'.")

        csv_content_to_process = None
        source_description = ""

        if csv_content_str_direct:
            csv_content_to_process = csv_content_str_direct
            source_description = "direct CSV content in HTTP payload"
            active_logger.info(f"Received direct CSV content for processing.")
        
        elif csv_gcs_path:
            source_description = f"GCS path {csv_gcs_path} from HTTP payload"
            active_logger.info(f"Fetching CSV content from GCS path: {csv_gcs_path}")
            if not csv_gcs_path.startswith("gs://"):
                active_logger.error("Invalid GCS path format for 'csv_gcs_path'. Must start with 'gs://'.")
                return ("Invalid 'csv_gcs_path' format. Must start with 'gs://'.", 400)
            
            try:
                # Simple split, assumes bucket name does not contain '/'
                bucket_name_http, blob_name_http = csv_gcs_path[5:].split("/", 1)
                storage_client_http = storage.Client() # Initialize client
                bucket_http = storage_client_http.bucket(bucket_name_http)
                blob_http = bucket_http.blob(blob_name_http)

                if not blob_http.exists():
                    active_logger.error(f"File not found in GCS via HTTP trigger: {csv_gcs_path}")
                    return (f"File not found at GCS path: {csv_gcs_path}", 404)
                
                csv_content_to_process = blob_http.download_as_text()
            except ValueError: # Handles if split fails
                active_logger.error(f"Invalid GCS path format in 'csv_gcs_path': {csv_gcs_path}")
                return ("Invalid 'csv_gcs_path' format. Example: gs://bucket/path/to/file.csv", 400)
            except Exception as e_gcs_dl:
                active_logger.error(f"Failed to download CSV from GCS path {csv_gcs_path} (HTTP trigger): {str(e_gcs_dl)}", exc_info=True)
                return (f"Failed to download CSV from GCS: {str(e_gcs_dl)}", 500)

        if csv_content_to_process is not None: # Explicitly check for None, as empty string could be valid CSV
            # Pass project_config_name to _process_csv_content
            _process_csv_content(csv_content_to_process, customer_id, project_config_name, active_logger)
            active_logger.info(f"Successfully processed category URLs from {source_description} (HTTP trigger).")
            return ({"status": "success", "message": f"Processed category URLs from {source_description}."}, 200)
        else:
            active_logger.error("No CSV content available for processing after checking direct and GCS path (HTTP trigger).")
            return ({"status": "error", "message": "No CSV content to process."}, 500)

    except Exception as e:
        # active_logger might not be set if error occurs before setup_logging
        current_logger = active_logger if 'active_logger' in locals() and active_logger != logger else logger
        current_logger.error(f"Error processing HTTP request: {str(e)}", exc_info=True)
        # Return a JSON response for HTTP errors
        return ({"status": "error", "message": str(e)}, 500)

def _process_csv_content(csv_content_str, customer_id, project_config_name, active_logger):
    """
    Helper function to parse CSV content and publish messages.
    project_config_name is the identifier for loading project-specific static configs.
    """
    # Load configurations
    try:
        customer_config = load_customer_config(customer_id)
        # project_config is used for CSV parsing rules (column index, header presence)
        project_config = load_project_config(project_config_name) 
    except FileNotFoundError as e:
        active_logger.error(f"Configuration file not found during _process_csv_content: {e}. Customer: {customer_id}, Project: {project_config_name}")
        # This is a critical error, stop processing for this CSV.
        raise ValueError(f"Configuration error for {customer_id}/{project_config_name}: {e}")

    gcp_project_id = customer_config.get("gcp_project_id", os.environ.get("GCP_PROJECT"))
    if not gcp_project_id:
        active_logger.error("GCP Project ID not found in customer configuration or environment.")
        raise ValueError("GCP Project ID configuration is missing.")

    # Get Pub/Sub topic name from environment or project_config as a fallback
    discover_main_urls_topic_name = os.environ.get(DISCOVER_MAIN_URLS_TOPIC_NAME_ENV)
    if not discover_main_urls_topic_name:
        # As a fallback, you could try to get it from project_config if it makes sense for your setup
        # discover_main_urls_topic_name = project_config.get("pubsub_topics", {}).get("discover_main_urls", DISCOVER_MAIN_URLS_TOPIC_NAME_DEFAULT)
        active_logger.error(f"Environment variable {DISCOVER_MAIN_URLS_TOPIC_NAME_ENV} for Pub/Sub topic is not set.")
        raise ValueError(f"Pub/Sub topic for discovering main URLs is not configured.")


    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(gcp_project_id, discover_main_urls_topic_name)

    csv_file = StringIO(csv_content_str)
    reader = csv.reader(csv_file) # Default delimiter is comma
    
    urls_published_count = 0
    rows_processed_count = 0
    header_skipped = False
    
    # Get CSV processing parameters from project_config
    url_column_index = project_config.get("category_url_csv_column_index", 0)
    has_header = project_config.get("category_url_csv_has_header", True)

    active_logger.info(f"CSV processing parameters for {project_config_name}: URL column index {url_column_index}, Has header {has_header}.")

    for row_number, row in enumerate(reader):
        rows_processed_count += 1
        if has_header and not header_skipped:
            header_skipped = True
            active_logger.info(f"Skipping header row: {row}")
            continue
        
        if not row: 
            active_logger.warning(f"Skipping empty row at line {row_number + 1}.")
            continue
            
        try:
            category_url = row[url_column_index].strip()
        except IndexError:
            active_logger.warning(f"Skipping row {row_number + 1} due to missing column index {url_column_index}. Row content: {row}")
            continue

        if not category_url:
            active_logger.warning(f"Skipping row {row_number + 1} due to empty URL in specified column {url_column_index}.")
            continue

        if not (category_url.startswith("http://") or category_url.startswith("https://")):
            active_logger.warning(f"Skipping invalid URL (does not start with http/https) from row {row_number + 1}: '{category_url}'.")
            continue
        
        # project_config_name is passed in the Pub/Sub message.
        # Downstream functions will use this to load the appropriate static and dynamic (AI-generated) configs.
        message_payload = {
            "customer": customer_id,
            "project": project_config_name, # This is the key for config loading downstream
            "category_url": category_url,
            "source_csv_ingest_timestamp": datetime.utcnow().isoformat() + "Z" # Good for traceability
        }
        message_data = json.dumps(message_payload).encode("utf-8")

        try:
            future = publisher.publish(topic_path, data=message_data)
            # .result() makes it synchronous and will raise an exception on failure.
            # For high throughput, you might manage futures in a list and check them periodically/at the end.
            future.result(timeout=60) # Adding a timeout for the publish operation
            urls_published_count += 1
            active_logger.debug(f"Published category URL to {discover_main_urls_topic_name}: {category_url}")
        except Exception as e_pub: # Catch Pub/Sub publishing errors specifically
            active_logger.error(f"Failed to publish category URL '{category_url}' to Pub/Sub topic '{discover_main_urls_topic_name}': {str(e_pub)}", exc_info=True)
            # Depending on requirements, you might:
            # 1. Continue processing other URLs and report errors at the end.
            # 2. Stop processing and raise an error (current behavior if .result() fails without a catch).
            # 3. Implement a retry mechanism for publishing here (though Pub/Sub client has some internal retries).
            # For now, logging the error and continuing with other URLs.

    active_logger.info(f"Finished processing CSV. Total rows processed: {rows_processed_count}. URLs published: {urls_published_count} to '{discover_main_urls_topic_name}'.")
    if urls_published_count == 0 and rows_processed_count > (1 if has_header else 0) : # Check if rows were processed but no URLs published
        active_logger.warning("No category URLs were published. Check CSV content, column index, header configuration, and URL validity for the processed rows.")