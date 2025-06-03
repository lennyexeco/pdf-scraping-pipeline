# src/functions/ingest_category_urls/main.py

import json
import base64
import logging
import os
import csv
from io import StringIO
from datetime import datetime

from google.cloud import pubsub_v1, storage
import functions_framework

from src.common.utils import setup_logging
from src.common.config import load_customer_config, load_project_config

logger = logging.getLogger(__name__)

DISCOVER_MAIN_URLS_TOPIC_NAME_ENV = "DISCOVER_MAIN_URLS_TOPIC"

def _process_csv_content(csv_content_str, customer_id, project_config_name, active_logger, source_description=""):
    """
    Helper function to parse CSV content and publish messages.
    Enhanced to add metadata and better error handling.
    """
    try:
        customer_config = load_customer_config(customer_id)
        project_config = load_project_config(project_config_name)
    except FileNotFoundError as e:
        active_logger.error(f"Configuration file not found during _process_csv_content: {e}. Customer: {customer_id}, Project: {project_config_name}")
        raise ValueError(f"Configuration error for {customer_id}/{project_config_name}: {e}")

    gcp_project_id = customer_config.get("gcp_project_id", os.environ.get("GCP_PROJECT"))
    if not gcp_project_id:
        active_logger.error("GCP Project ID not found in customer configuration or environment.")
        raise ValueError("GCP Project ID configuration is missing.")

    discover_main_urls_topic_name = os.environ.get(DISCOVER_MAIN_URLS_TOPIC_NAME_ENV)
    if not discover_main_urls_topic_name:
        discover_main_urls_topic_name = project_config.get("pubsub_topics", {}).get("discover_main_urls", "discover-main-urls-topic")
        active_logger.warning(f"{DISCOVER_MAIN_URLS_TOPIC_NAME_ENV} not set, using default/config: {discover_main_urls_topic_name}")

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(gcp_project_id, discover_main_urls_topic_name)

    csv_file = StringIO(csv_content_str)
    reader = csv.reader(csv_file)
    
    urls_published_count = 0
    rows_processed_count = 0
    header_skipped = False
    
    url_column_index = project_config.get("category_url_csv_column_index", 0)
    has_header = project_config.get("category_url_csv_has_header", True)

    active_logger.info(f"CSV processing for '{project_config_name}' from '{source_description}'. URL column index: {url_column_index}, Has header: {has_header}.")

    # NEW: Enhanced validation and metadata
    batch_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    
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
        
        # NEW: Enhanced message payload with additional metadata
        message_payload = {
            "customer": customer_id,
            "project": project_config_name,
            "category_url": category_url,
            
            # NEW: Enhanced metadata for better traceability
            "source_csv_ingest_timestamp": datetime.utcnow().isoformat() + "Z",
            "source_description": source_description,
            "batch_id": batch_id,
            "csv_row_number": row_number + 1,
            "original_csv_row": row,  # Store original row data for debugging
            
            # NEW: Configuration hints for discover_main_urls
            "ingest_metadata": {
                "url_column_index": url_column_index,
                "has_header": has_header,
                "total_rows_in_csv": None,  # Will be set after processing
                "processing_timestamp": datetime.utcnow().isoformat() + "Z"
            }
        }
        message_data = json.dumps(message_payload).encode("utf-8")

        try:
            future = publisher.publish(topic_path, data=message_data)
            future.result(timeout=60)
            urls_published_count += 1
            active_logger.debug(f"Published category URL to {discover_main_urls_topic_name}: {category_url}")
        except Exception as e_pub:
            active_logger.error(f"Failed to publish category URL '{category_url}' to Pub/Sub topic '{discover_main_urls_topic_name}': {str(e_pub)}", exc_info=True)

    active_logger.info(f"Finished processing CSV from '{source_description}'. Total rows: {rows_processed_count}. URLs published: {urls_published_count} to '{discover_main_urls_topic_name}'. Batch ID: {batch_id}")
    
    # NEW: Enhanced validation and warnings
    if urls_published_count == 0 and rows_processed_count > (1 if has_header else 0):
        active_logger.warning(f"No category URLs were published from '{source_description}'. Check CSV content, column index, header config, and URL validity.")
    
    # NEW: Log statistics for monitoring
    success_rate = (urls_published_count / max(rows_processed_count - (1 if has_header else 0), 1)) * 100
    active_logger.info(f"Processing statistics - Success rate: {success_rate:.1f}%, URLs published: {urls_published_count}, Total data rows: {rows_processed_count - (1 if has_header else 0)}")
    
    return {
        "urls_published": urls_published_count,
        "rows_processed": rows_processed_count,
        "batch_id": batch_id,
        "success_rate": success_rate
    }


@functions_framework.cloud_event
def ingest_category_urls_pubsub(cloud_event):
    """
    Enhanced Pub/Sub trigger with better error handling and metadata tracking.
    Expected message data:
    {
        "customer": "customer_id",
        "project": "project_config_name", 
        "csv_gcs_path": "gs://bucket_name/path/to/category_urls.csv"
    }
    """
    global logger
    active_logger = logger

    try:
        message_data_encoded = cloud_event.data["message"]["data"]
        message_data_decoded = base64.b64decode(message_data_encoded).decode("utf-8")
        payload = json.loads(message_data_decoded)
        
        # NEW: Get message metadata
        message_id = cloud_event.data["message"].get("messageId", "N/A")
        publish_time = cloud_event.data["message"].get("publishTime", "N/A")

        customer_id = payload.get("customer")
        project_config_name = payload.get("project")
        csv_gcs_path = payload.get("csv_gcs_path")

        if not all([customer_id, project_config_name, csv_gcs_path]):
            logger.error(f"Pub/Sub message missing required fields (customer, project, csv_gcs_path). Message ID: {message_id}. Payload: {payload}")
            raise ValueError("Invalid Pub/Sub message payload for ingest_category_urls_pubsub.")

        active_logger = setup_logging(customer_id, project_config_name)
        active_logger.info(f"Pub/Sub trigger for ingest_category_urls. Message ID: {message_id}, Publish time: {publish_time}. Customer: '{customer_id}', Project: '{project_config_name}', CSV GCS Path: '{csv_gcs_path}'.")

        if not csv_gcs_path.startswith("gs://"):
            active_logger.error(f"Invalid GCS path format for 'csv_gcs_path': {csv_gcs_path}. Must start with 'gs://'.")
            raise ValueError("Invalid 'csv_gcs_path' format.")

        try:
            bucket_name, blob_name = csv_gcs_path[5:].split("/", 1)
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)

            if not blob.exists():
                active_logger.error(f"File not found in GCS (from Pub/Sub trigger): {csv_gcs_path}")
                raise FileNotFoundError(f"File {csv_gcs_path} not found.")
            
            # NEW: Get file metadata
            blob.reload()
            file_size = blob.size
            file_updated = blob.updated
            active_logger.info(f"Processing CSV file: {csv_gcs_path} (Size: {file_size} bytes, Updated: {file_updated})")
            
            csv_content_str = blob.download_as_text()
            
            # NEW: Enhanced processing with statistics
            processing_stats = _process_csv_content(
                csv_content_str, 
                customer_id, 
                project_config_name, 
                active_logger, 
                source_description=f"Pub/Sub trigger (ID: {message_id}), GCS path: {csv_gcs_path}"
            )
            
            active_logger.info(f"Successfully processed category URLs from {csv_gcs_path} (Pub/Sub trigger). Stats: {processing_stats}")
            
            return {
                "status": "success", 
                "message": f"Processed {csv_gcs_path} via Pub/Sub.",
                "statistics": processing_stats,
                "message_id": message_id
            }

        except ValueError as ve:
            active_logger.error(f"ValueError during Pub/Sub triggered processing: {str(ve)}", exc_info=True)
            raise
        except FileNotFoundError as fnf:
            active_logger.error(f"FileNotFoundError during Pub/Sub triggered processing: {str(fnf)}", exc_info=True)
            raise
        except Exception as e_gcs_dl:
            active_logger.error(f"Failed to download/process CSV from {csv_gcs_path} (Pub/Sub trigger): {str(e_gcs_dl)}", exc_info=True)
            raise

    except Exception as e:
        current_logger = active_logger if active_logger != logger else logger
        current_logger.error(f"Error processing Pub/Sub event for ingest_category_urls: {str(e)}", exc_info=True)
        raise


# --- Optional: Keep GCS and HTTP triggers if needed for other workflows ---
# (These functions remain mostly unchanged, just adding message IDs and better logging)

@functions_framework.cloud_event
def ingest_category_urls_gcs_event(cloud_event):
    """
    Triggered by a GCS event when a CSV file is uploaded.
    Enhanced with better metadata tracking.
    """
    global logger
    active_logger = logger

    event_data = cloud_event.data
    bucket_name = event_data.get("bucket")
    file_name = event_data.get("name")
    event_type = event_data.get("eventType", "unknown")
    
    # NEW: Enhanced event logging
    logger.info(f"GCS event trigger: {event_type} for file: gs://{bucket_name}/{file_name}")

    if not bucket_name or not file_name:
        logger.error("GCS event missing bucket name or file name.")
        raise ValueError("Invalid GCS event data: missing bucket or file name.")

    path_parts = file_name.split('/')
    if len(path_parts) < 2:
        logger.error(f"Could not determine project_config_name from GCS path: {file_name}.")
        raise ValueError(f"Invalid GCS path format: {file_name}.")

    project_config_name = path_parts[0]
    customer_id = os.environ.get("CUSTOMER_ID", "harvey")

    active_logger = setup_logging(customer_id, project_config_name)
    active_logger.info(f"Processing GCS trigger ({event_type}) for customer '{customer_id}', project '{project_config_name}'. File: gs://{bucket_name}/{file_name}")

    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        
        if not blob.exists():
            active_logger.error(f"File not found in GCS: gs://{bucket_name}/{file_name}")
            raise FileNotFoundError(f"File gs://{bucket_name}/{file_name} not found.")

        csv_content_str = blob.download_as_text()
        processing_stats = _process_csv_content(
            csv_content_str, 
            customer_id, 
            project_config_name, 
            active_logger, 
            source_description=f"GCS trigger ({event_type}), file: {file_name}"
        )
        
        active_logger.info(f"Successfully processed category URLs from gs://{bucket_name}/{file_name} (GCS trigger). Stats: {processing_stats}")
        
        return {
            "status": "success", 
            "message": f"Processed {file_name} via GCS trigger.",
            "statistics": processing_stats
        }

    except Exception as e:
        active_logger.error(f"Error processing GCS event for file gs://{bucket_name}/{file_name}: {str(e)}", exc_info=True)
        raise


@functions_framework.http
def ingest_category_urls_http(request):
    """
    Enhanced HTTP trigger with better validation and response formatting.
    """
    global logger
    active_logger = logger

    if request.method != "POST":
        return ("Only POST requests are accepted", 405)

    try:
        request_json = request.get_json(silent=True)
        if not request_json:
            return ("Invalid JSON payload", 400)

        customer_id = request_json.get("customer")
        project_config_name = request_json.get("project")
        csv_gcs_path = request_json.get("csv_gcs_path")
        csv_content_str_direct = request_json.get("csv_content")

        if not customer_id or not project_config_name:
            return ("Missing 'customer' or 'project' in JSON payload", 400)
        
        active_logger = setup_logging(customer_id, project_config_name)
        active_logger.info(f"Processing HTTP trigger for customer '{customer_id}', project '{project_config_name}'.")

        if not csv_gcs_path and not csv_content_str_direct:
            active_logger.error("HTTP trigger: Either 'csv_gcs_path' or 'csv_content' must be provided.")
            return ("Either 'csv_gcs_path' or 'csv_content' must be provided.", 400)

        csv_content_to_process = None
        source_desc = ""

        if csv_content_str_direct:
            csv_content_to_process = csv_content_str_direct
            source_desc = "direct CSV content in HTTP payload"
            active_logger.info("Received direct CSV content via HTTP.")
        
        elif csv_gcs_path:
            source_desc = f"GCS path {csv_gcs_path} from HTTP payload"
            active_logger.info(f"Fetching CSV content from GCS path (HTTP): {csv_gcs_path}")
            if not csv_gcs_path.startswith("gs://"):
                active_logger.error(f"Invalid GCS path format (HTTP): {csv_gcs_path}")
                return ("Invalid 'csv_gcs_path' format. Must start with 'gs://'.", 400)
            
            try:
                bucket_name_http, blob_name_http = csv_gcs_path[5:].split("/", 1)
                storage_client_http = storage.Client()
                bucket_http = storage_client_http.bucket(bucket_name_http)
                blob_http = bucket_http.blob(blob_name_http)

                if not blob_http.exists():
                    active_logger.error(f"File not found in GCS (HTTP): {csv_gcs_path}")
                    return (f"File not found at GCS path: {csv_gcs_path}", 404)
                
                csv_content_to_process = blob_http.download_as_text()
            except Exception as e_gcs_dl_http:
                active_logger.error(f"Failed to download CSV from GCS (HTTP): {csv_gcs_path}: {str(e_gcs_dl_http)}", exc_info=True)
                return (f"Failed to download CSV from GCS: {str(e_gcs_dl_http)}", 500)

        if csv_content_to_process is not None:
            processing_stats = _process_csv_content(
                csv_content_to_process, 
                customer_id, 
                project_config_name, 
                active_logger, 
                source_description=source_desc
            )
            
            active_logger.info(f"Successfully processed category URLs from {source_desc} (HTTP trigger). Stats: {processing_stats}")
            
            return ({
                "status": "success", 
                "message": f"Processed category URLs from {source_desc}.",
                "statistics": processing_stats
            }, 200)
        else:
            active_logger.error("No CSV content to process (HTTP trigger).")
            return ({"status": "error", "message": "No CSV content to process."}, 500)

    except Exception as e:
        current_logger = active_logger if active_logger != logger else logger
        current_logger.error(f"Error processing HTTP request for ingest_category_urls: {str(e)}", exc_info=True)
        return ({"status": "error", "message": str(e)}, 500)