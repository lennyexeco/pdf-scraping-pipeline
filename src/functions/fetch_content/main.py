# src/functions/fetch_content/main.py

import json
import base64
import logging
import os
import re
import time
from datetime import datetime
from urllib.parse import urljoin, urlparse

import functions_framework
import requests
from bs4 import BeautifulSoup
from google.cloud import firestore, pubsub_v1, storage

# Assuming these are in src.common or accessible in PYTHONPATH
from src.common.config import load_customer_config, load_dynamic_site_config, load_project_config # Added load_project_config for static access
from src.common.helpers import sanitize_filename
from src.common.utils import setup_logging

logger = logging.getLogger(__name__)

# Configuration for retries (can be environment variables)
MAX_GCS_RETRIES = int(os.environ.get("MAX_GCS_RETRIES", 3))
MAX_FIRESTORE_RETRIES = int(os.environ.get("MAX_FIRESTORE_RETRIES", 3))
MAX_PDF_DOWNLOAD_RETRIES = int(os.environ.get("MAX_PDF_DOWNLOAD_RETRIES", 3))
RETRY_BACKOFF = int(os.environ.get("RETRY_BACKOFF", 2)) # seconds

NEXT_STEP_GENERATE_XML_TOPIC_NAME = "generate-xml-topic"
RETRY_TOPIC_NAME = "retry-pipeline-topic" # Make sure this matches your actual retry topic name


def publish_to_retry_topic(publisher_client, gcp_project_id, payload, active_logger):
    """Publishes a message to the retry topic."""
    try:
        topic_path = publisher_client.topic_path(gcp_project_id, RETRY_TOPIC_NAME)
        # Ensure the original pubsub_message (if it contained retry_count) is used as a base for the new retry_count
        base_retry_count = payload.get("original_pubsub_message", {}).get("retry_count", 0)
        
        # If payload itself has retry_count (e.g. from a direct call, not via critical error path), use that
        if "retry_count" in payload:
             base_retry_count = payload.get("retry_count",0)


        # Ensure the error_message in the payload is what gets sent to the retry topic
        error_message_for_retry = payload.get("error_message", "Unknown error for retry")

        retry_payload_for_topic = {
            "customer": payload.get("customer"),
            "project": payload.get("project"),
            "identifier": payload.get("identifier"),
            "main_url": payload.get("main_url"),
            "error_message": error_message_for_retry,
            "stage": payload.get("stage"),
            # Increment retry_count from the payload being retried
            "retry_count": base_retry_count + 1,
             # Pass along the original message if it's available and different from current payload
            "original_pubsub_message": payload.get("original_pubsub_message", payload)
        }


        future = publisher_client.publish(topic_path, json.dumps(retry_payload_for_topic, default=str).encode("utf-8"))
        future.result(timeout=60) # Adding a timeout
        active_logger.info(f"Published error to retry topic: {RETRY_TOPIC_NAME} for identifier {payload.get('identifier')}, stage {payload.get('stage')}, next retry count {retry_payload_for_topic['retry_count']}")
    except Exception as e:
        active_logger.error(f"Failed to publish to retry topic {RETRY_TOPIC_NAME} for identifier {payload.get('identifier')}: {e}", exc_info=True)


@functions_framework.cloud_event
def fetch_content(cloud_event):
    """
    Fetches HTML content for a MainURL, makes links absolute, downloads linked PDFs,
    stores content in GCS, and updates Firestore.
    """
    active_logger = logger # Default logger
    pubsub_message_data_encoded = cloud_event.data.get("message", {}).get("data")
    if not pubsub_message_data_encoded:
        active_logger.error("No 'data' in Pub/Sub message envelope.")
        # Not raising ValueError here to allow function to exit cleanly if this is an unexpected event type
        return {"status": "error", "message": "No 'data' in Pub/Sub message envelope."}


    pubsub_message = {}
    gcp_project_id_env = os.environ.get("GCP_PROJECT")
    customer_id = "unknown_customer"
    project_id_config_name = "unknown_project"
    identifier = "unknown_identifier"
    db = None # Initialize db client
    doc_ref = None # Initialize doc_ref
    publisher_client = None # Initialize publisher_client

    try:
        pubsub_message = json.loads(base64.b64decode(pubsub_message_data_encoded).decode("utf-8"))
        customer_id = pubsub_message.get("customer")
        project_id_config_name = pubsub_message.get("project")
        main_url = pubsub_message.get("main_url")
        identifier = pubsub_message.get("identifier")

        if not all([customer_id, project_id_config_name, main_url, identifier]):
            missing_fields = [
                k for k, v in {"customer": customer_id, "project": project_id_config_name,
                               "main_url": main_url, "identifier": identifier}.items() if not v
            ]
            # Use default logger if active_logger not set
            (active_logger if active_logger != logger else logger).error(
                f"Missing required Pub/Sub fields: {', '.join(missing_fields)}. Data: {str(pubsub_message)[:500]}"
            )
            # Return a non-error HTTP status for Pub/Sub to ack the message and not retry invalid input
            return {"status": "error_bad_request", "message": f"Missing required Pub/Sub fields: {', '.join(missing_fields)}"}


        active_logger = setup_logging(customer_id, project_id_config_name)
        active_logger.info(f"Starting fetch_content for identifier: {identifier}, MainURL: {main_url}")

        customer_config = load_customer_config(customer_id)
        gcp_project_id = customer_config.get("gcp_project_id", gcp_project_id_env)

        if not gcp_project_id:
            active_logger.error("GCP Project ID not configured.")
            raise ValueError("GCP Project ID not configured.") # This is a config error, so raise

        # Load static project config first to get firestore_database_id for DB client initialization
        # This assumes load_project_config doesn't need a db client itself.
        temp_static_project_config = load_project_config(project_id_config_name)
        firestore_db_id_from_static = temp_static_project_config.get("firestore_database_id", "(default)")

        db_options = {"project": gcp_project_id}
        if firestore_db_id_from_static != "(default)":
             db_options["database"] = firestore_db_id_from_static
        db = firestore.Client(**db_options)

        # Load dynamic project configuration (merged static + AI schema from Firestore)
        project_config = load_dynamic_site_config(db, project_id_config_name, active_logger)
        
        gcs_bucket_name = project_config.get("gcs_bucket")
        firestore_collection_name = project_config.get("firestore_collection") # Should come from merged config
        image_url_fix_config = project_config.get("image_url_fix", {})
        gcs_html_path_template = project_config.get("gcs_html_path_template", "{project}/html/{identifier}.html")
        gcs_pdf_path_template = project_config.get("gcs_pdf_path_template", "{project}/pdfs/{identifier}/{date_str}/{pdf_filename}") # Added date_str
        default_date_str = datetime.now().strftime('%Y%m%d') # Used for PDF path if template includes {date_str}

        if not all([gcs_bucket_name, firestore_collection_name]):
            active_logger.error("Missing critical GCS/Firestore configuration from project_config.")
            raise ValueError("Missing GCS bucket or Firestore collection configuration.")


        storage_client = storage.Client(project=gcp_project_id)
        publisher_client = pubsub_v1.PublisherClient() # Initialize here for consistent use
        doc_ref = db.collection(firestore_collection_name).document(identifier)

        # 1. Fetch HTML content of MainURL
        html_content = None
        try:
            scrape_timeout = project_config.get("fetch_content_scrape_timeout", 60) # Configurable timeout
            response = requests.get(main_url, timeout=scrape_timeout, headers={'User-Agent': 'Google-Cloud-Function-Scraper/1.0'})
            response.raise_for_status()
            # Try to decode with detected encoding, fallback to response.text (which uses 'ISO-8859-1' if no charset in header)
            try:
                html_content = response.content.decode(response.encoding if response.encoding else 'utf-8', errors='replace')
            except (LookupError, TypeError): # If encoding is None or invalid
                 html_content = response.text # relies on requests' default decoding
            active_logger.info(f"Successfully fetched HTML for {main_url}")
        except requests.exceptions.RequestException as e:
            error_message = f"Failed to fetch HTML for {main_url}: {str(e)}"
            active_logger.error(error_message, exc_info=True)
            if doc_ref:
                 doc_ref.update({"processing_status": "error_content_fetch_html", "last_error": str(e), "last_updated": firestore.SERVER_TIMESTAMP})
            # Construct payload for retry topic, ensuring it has necessary fields
            retry_payload = {
                "customer": customer_id, "project": project_id_config_name, "identifier": identifier,
                "main_url": main_url, "error_message": str(e),
                "stage": "fetch_content_html_download",
                "original_pubsub_message": pubsub_message # Pass along the original message for context
            }
            publish_to_retry_topic(publisher_client, gcp_project_id, retry_payload, active_logger)
            return {"status": "error_sent_to_retry", "message": error_message}


        if not html_content or not html_content.strip():
            active_logger.warning(f"No HTML content retrieved or content is empty for {main_url}.")
            if doc_ref:
                doc_ref.update({"processing_status": "error_content_empty_html", "last_error": "No HTML content", "last_updated": firestore.SERVER_TIMESTAMP})
            # No retry for empty content unless a specific strategy is desired
            return {"status": "success_processed_empty_html", "message": "No HTML content to process."}


        # 2. URL Absolutization
        soup = BeautifulSoup(html_content, 'html.parser')
        tags_and_attributes = {
            'img': 'src', 'link': 'href', 'a': 'href', 'script': 'src',
            'iframe': 'src', 'source': 'src', 'track': 'src',
            # Consider 'form': 'action' if forms are important
        }
        images_fixed_count = 0
        # These image_url_fix_config settings should ideally come from project_config
        use_regex_fix = image_url_fix_config.get("use_regex_fix", False)
        base_url_for_fix = image_url_fix_config.get("base_url", main_url) # main_url is a good default for base
        regex_pattern_str = image_url_fix_config.get("regex_pattern")
        regex_replacement_str = image_url_fix_config.get("regex_replacement")


        active_logger.info(f"Starting URL absolutization for {main_url}. Strategy: {'regex' if use_regex_fix and regex_pattern_str else 'urljoin'}.")
        for tag_name, attr_name in tags_and_attributes.items():
            for tag_element in soup.find_all(tag_name):
                if tag_element.has_attr(attr_name):
                    original_url = tag_element[attr_name]
                    if original_url and not original_url.startswith(('http://', 'https://', 'data:', '#', 'mailto:', 'tel:')):
                        absolute_url = None
                        if use_regex_fix and regex_pattern_str and regex_replacement_str:
                            # Ensure the pattern and replacement logic correctly forms a full or relative URL
                            # This part needs careful implementation based on how regex_pattern is intended to work
                            # For example, if regex extracts a path, and replacement forms a new base.
                            # A simple example (might not be what you need):
                            # if re.match(regex_pattern_str, original_url):
                            #    processed_rel_url = re.sub(regex_pattern_str, regex_replacement_str, original_url)
                            #    absolute_url = urljoin(base_url_for_fix, processed_rel_url)
                            # else:
                            #    absolute_url = urljoin(base_url_for_fix, original_url) # Fallback if regex doesn't match
                            # For now, let's assume urljoin is primary unless regex logic is very specific
                            absolute_url = urljoin(base_url_for_fix, original_url) # Defaulting to urljoin for simplicity
                        else:
                            absolute_url = urljoin(main_url, original_url) # Use main_url as base if no special regex fix

                        if absolute_url:
                            tag_element[attr_name] = absolute_url
                            if tag_name == 'img': images_fixed_count +=1
        active_logger.info(f"Absolutized URLs. Images fixed/updated: {images_fixed_count}")
        processed_html_content = soup.prettify(formatter="html5") # Use html5 formatter for better modern HTML

        # 3. PDF Discovery and Download
        pdf_gcs_paths = []
        pdf_links_found = []
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            # Ensure href is a string before calling lower() or endswith()
            if isinstance(href, str):
                parsed_href = urlparse(href)
                if parsed_href.scheme and parsed_href.netloc and href.lower().endswith('.pdf'):
                    pdf_links_found.append(href)
            else:
                active_logger.warning(f"Found non-string href in <a> tag: {href} on page {main_url}")

        active_logger.info(f"Found {len(pdf_links_found)} potential PDF links for {main_url}.")

        for pdf_index, pdf_url in enumerate(pdf_links_found):
            try:
                pdf_filename_base = os.path.basename(urlparse(pdf_url).path)
                if not pdf_filename_base: # Handle cases like "domain.com/resource.pdf?query" where path is just "/"
                    pdf_filename_base = f"downloaded_pdf_{pdf_index + 1}.pdf"

                # Ensure unique element in filename using index if sanitize_filename doesn't guarantee uniqueness with fallback
                pdf_filename = sanitize_filename(pdf_filename_base, f"{identifier}_pdf_{pdf_index + 1}", logger_instance=active_logger)
                if not pdf_filename.lower().endswith(".pdf"):
                    pdf_filename += ".pdf"

                # Using the gcs_pdf_path_template from project_config
                current_gcs_pdf_path = gcs_pdf_path_template.format(
                    project=project_id_config_name,
                    identifier=identifier,
                    pdf_filename=pdf_filename,
                    date_str=default_date_str # Use the date string
                )
                pdf_blob = storage_client.bucket(gcs_bucket_name).blob(current_gcs_pdf_path)

                for attempt in range(MAX_PDF_DOWNLOAD_RETRIES):
                    try:
                        pdf_response = requests.get(pdf_url, timeout=project_config.get("fetch_content_pdf_timeout", 120), stream=True, headers={'User-Agent': 'Google-Cloud-Function-Scraper/1.0'})
                        pdf_response.raise_for_status()
                        pdf_blob.upload_from_file(pdf_response.raw, content_type='application/pdf')
                        gcs_uri = f"gs://{gcs_bucket_name}/{current_gcs_pdf_path}"
                        pdf_gcs_paths.append(gcs_uri)
                        active_logger.info(f"Successfully downloaded and stored PDF from {pdf_url} to {gcs_uri}")
                        break # Success, exit attempt loop
                    except requests.exceptions.RequestException as e_pdf_req:
                        active_logger.warning(f"PDF download attempt {attempt + 1}/{MAX_PDF_DOWNLOAD_RETRIES} for {pdf_url} failed: {e_pdf_req}")
                        if attempt == MAX_PDF_DOWNLOAD_RETRIES - 1:
                            active_logger.error(f"Max retries reached for PDF download {pdf_url}. Skipping this PDF.")
                            # Optionally log this failure to Firestore for the main item
                        else:
                            time.sleep(RETRY_BACKOFF * (2 ** attempt))
                    except Exception as e_gcs_pdf_upload: # Catch other errors like GCS upload issues
                        active_logger.warning(f"GCS PDF upload attempt {attempt + 1}/{MAX_PDF_DOWNLOAD_RETRIES} for {pdf_url} to {current_gcs_pdf_path} failed: {e_gcs_pdf_upload}")
                        if attempt == MAX_PDF_DOWNLOAD_RETRIES - 1:
                            active_logger.error(f"Max retries for GCS PDF upload {pdf_url}. Skipping this PDF.")
                        else:
                            time.sleep(RETRY_BACKOFF * (2 ** attempt))
            except Exception as e_outer_pdf_loop:
                 active_logger.error(f"Outer error processing PDF URL {pdf_url}: {e_outer_pdf_loop}", exc_info=True)


        # 4. Store Processed HTML to GCS
        doc_snapshot = None
        if doc_ref:
            doc_snapshot = doc_ref.get()
        doc_data_for_filename = doc_snapshot.to_dict() if doc_snapshot and doc_snapshot.exists else {}
        
        # --- UPDATED HTML Filename Logic ---
        # Get the configured field name for the title from project_config
        # Add "title_field_in_firestore_for_filename" to your project_config.json if you want this configurable
        # Default to "title" if not specified in config. "title" should be the key used in Firestore.
        title_field_key = project_config.get("title_field_in_firestore_for_filename", "title")
        html_filename_base = doc_data_for_filename.get(title_field_key, identifier)
        # --- END UPDATED HTML Filename Logic ---

        html_filename_sanitized = sanitize_filename(html_filename_base, identifier, logger_instance=active_logger)
        if not html_filename_sanitized.lower().endswith((".html", ".htm")):
             html_filename_sanitized += ".html"
        
        # Determine final GCS HTML path using template
        # Example template: "{project}/html/{date_str}/{identifier}/{filename}.html"
        # Ensure your template uses available format keys.
        final_gcs_html_path = gcs_html_path_template.format(
            project=project_id_config_name,
            identifier=identifier,
            filename=html_filename_sanitized, # If your template uses {filename}
            date_str=default_date_str # If your template uses {date_str}
        )

        html_blob = storage_client.bucket(gcs_bucket_name).blob(final_gcs_html_path)
        uploaded_html_gcs_uri = None

        for attempt in range(MAX_GCS_RETRIES):
            try:
                html_blob.upload_from_string(processed_html_content, content_type='text/html; charset=utf-8')
                uploaded_html_gcs_uri = f"gs://{gcs_bucket_name}/{final_gcs_html_path}"
                active_logger.info(f"Stored processed HTML for {identifier} to {uploaded_html_gcs_uri}")
                break
            except Exception as e_gcs_html:
                active_logger.warning(f"GCS HTML upload attempt {attempt + 1}/{MAX_GCS_RETRIES} for {identifier} to {final_gcs_html_path} failed: {e_gcs_html}")
                if attempt == MAX_GCS_RETRIES - 1:
                    active_logger.error(f"Max retries for GCS HTML upload for {identifier}. Aborting GCS HTML store.")
                    if doc_ref:
                        doc_ref.update({"processing_status": "error_content_gcs_html_upload", "last_error": str(e_gcs_html), "last_updated": firestore.SERVER_TIMESTAMP})
                    retry_payload = {
                        "customer": customer_id, "project": project_id_config_name, "identifier": identifier,
                        "main_url": main_url, "error_message": str(e_gcs_html),
                        "stage": "fetch_content_gcs_html_upload",
                        "original_pubsub_message": pubsub_message
                    }
                    publish_to_retry_topic(publisher_client, gcp_project_id, retry_payload, active_logger)
                    return {"status": "error_sent_to_retry", "message": f"GCS HTML upload failed for {identifier}"}
                time.sleep(RETRY_BACKOFF * (2 ** attempt))

        if not uploaded_html_gcs_uri:
            active_logger.error(f"HTML GCS path was not set for {identifier}, likely due to upload failure. Firestore will not be updated with path.")
            # This path should ideally not be reached if retry logic above works.
            return {"status": "error_internal", "message": "HTML GCS URI not set after retries."}


        # 5. Update Firestore document
        firestore_update_data = {
            "html_gcs_path": uploaded_html_gcs_uri,
            "pdf_gcs_paths": pdf_gcs_paths, # Store list of GCS URIs for PDFs
            "processing_status": "content_fetched",
            "content_fetched_timestamp": firestore.SERVER_TIMESTAMP,
            "images_fixed_count": images_fixed_count,
            "last_updated": firestore.SERVER_TIMESTAMP,
            "last_error": firestore.DELETE_FIELD # Clear any previous error
        }

        if doc_ref:
            for attempt in range(MAX_FIRESTORE_RETRIES):
                try:
                    doc_ref.update(firestore_update_data)
                    active_logger.info(f"Successfully updated Firestore for {identifier} with content paths.")
                    break
                except Exception as e_fs:
                    active_logger.warning(f"Firestore update attempt {attempt + 1}/{MAX_FIRESTORE_RETRIES} for {identifier} failed: {e_fs}")
                    if attempt == MAX_FIRESTORE_RETRIES - 1:
                        active_logger.error(f"Max retries for Firestore update for {identifier}. State may be inconsistent.")
                        # Publish to retry if Firestore update fails critically, as this is a key step
                        retry_payload = {
                            "customer": customer_id, "project": project_id_config_name, "identifier": identifier,
                            "main_url": main_url, "error_message": str(e_fs),
                            "stage": "fetch_content_firestore_update",
                             "item_data_snapshot": {**doc_data_for_filename, **firestore_update_data}, # snapshot of what we tried to write
                            "original_pubsub_message": pubsub_message
                        }
                        publish_to_retry_topic(publisher_client, gcp_project_id, retry_payload, active_logger)
                        return {"status": "error_sent_to_retry", "message": f"Firestore update failed for {identifier}"}
                    time.sleep(RETRY_BACKOFF * (2 ** attempt))
        else: # Should not happen if identifier is present
            active_logger.error(f"doc_ref not initialized for Firestore update for identifier {identifier}.")
            raise ValueError(f"doc_ref not initialized for {identifier}")


        # 6. Publish to next step (generate-xml)
        next_step_payload = {
            "customer": customer_id,
            "project": project_id_config_name,
            "identifier": identifier,
            # Pass date if it's relevant for XML generation or subsequent reporting context
            "date": default_date_str # Or a date from pubsub_message if it was passed along
        }
        if publisher_client:
            topic_path_next_step = publisher_client.topic_path(gcp_project_id, NEXT_STEP_GENERATE_XML_TOPIC_NAME)
            future = publisher_client.publish(topic_path_next_step, json.dumps(next_step_payload).encode("utf-8"))
            future.result(timeout=60) # Wait for publish to complete
            active_logger.info(f"Published to '{NEXT_STEP_GENERATE_XML_TOPIC_NAME}' for {identifier}. Message ID: {future.message_id}")
        else: # Should not happen if initialization is correct
            active_logger.error("publisher_client not initialized before publishing to next step.")


        return {"status": "success", "identifier": identifier, "html_gcs_path": uploaded_html_gcs_uri}

    except Exception as e:
        # Use active_logger if set, otherwise default module logger
        current_logger = active_logger if active_logger != logger else logger
        error_message_critical = f"Critical error in fetch_content for identifier '{identifier}', pubsub_message: '{str(pubsub_message)[:500]}': {str(e)}"
        current_logger.error(error_message_critical, exc_info=True)
        
        if doc_ref: # Attempt to update Firestore with critical error status
            try:
                doc_ref.update({"processing_status": "error_content_critical", "last_error": str(e), "last_updated": firestore.SERVER_TIMESTAMP})
            except Exception as e_fs_crit:
                current_logger.error(f"Failed to update Firestore with critical error details for {identifier}: {e_fs_crit}")

        # Construct payload for retry topic
        # Ensure publisher_client is initialized for the retry mechanism
        if publisher_client is None and 'gcp_project_id' in locals() and gcp_project_id:
            try:
                publisher_client = pubsub_v1.PublisherClient()
            except Exception as e_pub_init:
                current_logger.error(f"Failed to initialize PublisherClient for critical error retry: {e_pub_init}")
        
        if publisher_client and 'gcp_project_id' in locals() and gcp_project_id:
            retry_payload_critical = {
                "customer": customer_id, # Use parsed customer_id if available
                "project": project_id_config_name, # Use parsed project_id_config_name if available
                "identifier": identifier, # Use parsed identifier if available
                "main_url": pubsub_message.get("main_url", "N/A_in_critical_error"),
                "error_message": str(e),
                "stage": "fetch_content_critical",
                "original_pubsub_message": pubsub_message
            }
            publish_to_retry_topic(publisher_client, gcp_project_id, retry_payload_critical, current_logger)
        else:
            current_logger.error("Cannot publish critical error to retry topic: publisher_client or GCP Project ID not available.")
        
        # Re-raise the exception to mark the Cloud Function execution as failed
        # This will allow Pub/Sub to handle its own retry policy for the original message if configured.
        # However, our custom retry through `publish_to_retry_topic` is more controlled.
        # Consider if raising here is always desired if custom retry is also happening.
        # For now, keeping it to signal failure.
        raise