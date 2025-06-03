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

from src.common.config import load_customer_config, load_dynamic_site_config, load_project_config 
from src.common.helpers import sanitize_filename
from src.common.utils import setup_logging

logger = logging.getLogger(__name__)

MAX_GCS_RETRIES = int(os.environ.get("MAX_GCS_RETRIES", 3))
MAX_FIRESTORE_RETRIES = int(os.environ.get("MAX_FIRESTORE_RETRIES", 3))
MAX_PDF_DOWNLOAD_RETRIES = int(os.environ.get("MAX_PDF_DOWNLOAD_RETRIES", 3))
RETRY_BACKOFF = int(os.environ.get("RETRY_BACKOFF", 2)) 

NEXT_STEP_GENERATE_XML_TOPIC_NAME = "generate-xml-topic"
RETRY_TOPIC_NAME = "retry-pipeline-topic" 


def publish_to_retry_topic(publisher_client, gcp_project_id, payload, active_logger):
    """Publishes a message to the retry topic."""
    try:
        topic_path = publisher_client.topic_path(gcp_project_id, RETRY_TOPIC_NAME)
        base_retry_count = payload.get("original_pubsub_message", {}).get("retry_count", 0)
        
        if "retry_count" in payload:
             base_retry_count = payload.get("retry_count",0)

        error_message_for_retry = payload.get("error_message", "Unknown error for retry")
        retry_payload_for_topic = {
            "customer": payload.get("customer"),
            "project": payload.get("project"),
            "identifier": payload.get("identifier"),
            "main_url": payload.get("main_url"),
            "error_message": error_message_for_retry,
            "stage": payload.get("stage"),
            "retry_count": base_retry_count + 1,
            "original_pubsub_message": payload.get("original_pubsub_message", payload)
        }
        future = publisher_client.publish(topic_path, json.dumps(retry_payload_for_topic, default=str).encode("utf-8"))
        future.result(timeout=60) 
        active_logger.info(f"Published error to retry topic: {RETRY_TOPIC_NAME} for identifier {payload.get('identifier')}, stage {payload.get('stage')}, next retry count {retry_payload_for_topic['retry_count']}")
    except Exception as e:
        active_logger.error(f"Failed to publish to retry topic {RETRY_TOPIC_NAME} for identifier {payload.get('identifier')}: {e}", exc_info=True)


def is_meaningful_pdf_attachment(link_element, href):
    """
    Determines if a PDF link is a meaningful document attachment vs navigation.
    
    Args:
        link_element: BeautifulSoup element for the <a> tag
        href: The href URL
        
    Returns:
        bool: True if this appears to be a document attachment
    """
    link_text = link_element.get_text(strip=True).lower()
    
    # Strong indicators this is a document attachment
    attachment_indicators = [
        'download', 'pdf', 'attachment', 'report', 'document', 'guide',
        'statement', 'consultation', 'media release', 'regulatory guide',
        'information sheet', 'instrument', 'annual report'
    ]
    
    # Strong indicators this is navigation/generic
    navigation_indicators = [
        'view all', 'more pdfs', 'archive', 'see all', 'browse',
        'index', 'list', 'menu', 'navigation'
    ]
    
    # Check for attachment indicators
    is_attachment = any(indicator in link_text for indicator in attachment_indicators)
    
    # Check for navigation indicators  
    is_navigation = any(indicator in link_text for indicator in navigation_indicators)
    
    # Length-based heuristics
    if len(link_text) > 15:  # Longer text suggests meaningful content
        is_attachment = True
    elif len(link_text) < 5:  # Very short text likely navigation
        is_navigation = True
    
    # Check parent context
    parent_text = ""
    if link_element.parent:
        parent_text = link_element.parent.get_text(strip=True).lower()
        if any(indicator in parent_text for indicator in attachment_indicators):
            is_attachment = True
    
    return is_attachment and not is_navigation


@functions_framework.cloud_event
def fetch_content(cloud_event):
    """
    Enhanced to handle different document types with smart PDF processing.
    Fetches HTML content, makes links absolute, downloads meaningful PDFs, 
    stores content in GCS using indexed_filename_base, and updates Firestore.
    """
    active_logger = logger 
    pubsub_message_data_encoded = cloud_event.data.get("message", {}).get("data")
    if not pubsub_message_data_encoded:
        active_logger.error("No 'data' in Pub/Sub message envelope.")
        return {"status": "error", "message": "No 'data' in Pub/Sub message envelope."}

    pubsub_message = {}
    gcp_project_id_env = os.environ.get("GCP_PROJECT")
    customer_id = "unknown_customer"
    project_id_config_name = "unknown_project"
    identifier = "unknown_identifier" 
    main_url = "unknown_url"
    db = None 
    doc_ref = None
    publisher_client = None 

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
            (active_logger if active_logger != logger else logger).error(
                f"Missing required Pub/Sub fields: {', '.join(missing_fields)}. Data: {str(pubsub_message)[:500]}"
            )
            return {"status": "error_bad_request", "message": f"Missing required Pub/Sub fields: {', '.join(missing_fields)}"}

        active_logger = setup_logging(customer_id, project_id_config_name)
        active_logger.info(f"Starting fetch_content for identifier: {identifier}, MainURL: {main_url}")

        customer_config = load_customer_config(customer_id)
        gcp_project_id = customer_config.get("gcp_project_id", gcp_project_id_env)

        if not gcp_project_id:
            active_logger.error("GCP Project ID not configured.")
            raise ValueError("GCP Project ID not configured.")

        temp_static_project_config = load_project_config(project_id_config_name)
        firestore_db_id_from_static = temp_static_project_config.get("firestore_database_id", "(default)")

        db_options = {"project": gcp_project_id}
        if firestore_db_id_from_static != "(default)":
             db_options["database"] = firestore_db_id_from_static
        db = firestore.Client(**db_options)

        project_config = load_dynamic_site_config(db, project_id_config_name, active_logger)
        
        gcs_bucket_name = project_config.get("gcs_bucket")
        firestore_collection_name = project_config.get("firestore_collection")
        
        gcs_html_path_template = project_config.get("gcs_html_path_template", "{project}/html/{date_str}/{indexed_filename_base}.html")
        gcs_pdf_path_template = project_config.get("gcs_pdf_path_template", "{project}/pdfs/{date_str}/{indexed_filename_base}_pdf_{pdf_filename_suffix}.pdf")
        default_date_str = datetime.now().strftime('%Y%m%d')

        if not all([gcs_bucket_name, firestore_collection_name]):
            active_logger.error("Missing critical GCS/Firestore configuration from project_config.")
            raise ValueError("Missing GCS bucket or Firestore collection configuration.")

        storage_client = storage.Client(project=gcp_project_id)
        publisher_client = pubsub_v1.PublisherClient()
        doc_ref = db.collection(firestore_collection_name).document(identifier)

        # --- Get document data from Firestore ---
        doc_snapshot = doc_ref.get()
        if not doc_snapshot.exists:
            active_logger.error(f"Firestore document {identifier} not found.")
            raise ValueError(f"Firestore document {identifier} not found.")
        
        doc_data = doc_snapshot.to_dict()
        indexed_filename_base = doc_data.get("indexed_filename_base")
        
        # NEW: Get document type to determine processing path
        document_type = doc_data.get("document_type", "HTML_DOCUMENT")
        
        if not indexed_filename_base:
            active_logger.error(f"indexed_filename_base not found in Firestore document {identifier}.")
            project_abbr = project_config.get("project_abbreviation", "DOC")
            indexed_filename_base = f"{project_abbr}_{identifier[:8]}"
            active_logger.warning(f"Using fallback indexed_filename_base: {indexed_filename_base}")

        active_logger.info(f"Processing {document_type} for {main_url} with filename base: {indexed_filename_base}")

        # --- NEW: Handle different document types ---
        if document_type == "PDF_DOCUMENT":
            # This is a direct PDF link - download it directly
            active_logger.info(f"Downloading direct PDF: {main_url}")
            pdf_gcs_paths = []
            
            try:
                # Use indexed filename for direct PDF
                pdf_filename_suffix_for_template = "direct"  # Indicate this is the main PDF
                
                current_gcs_pdf_path = gcs_pdf_path_template.format(
                    project=project_id_config_name,
                    identifier=identifier,
                    indexed_filename_base=indexed_filename_base,
                    pdf_filename_suffix=pdf_filename_suffix_for_template,
                    date_str=default_date_str
                )
                
                # Download PDF
                pdf_response = requests.get(
                    main_url, 
                    timeout=project_config.get("fetch_content_pdf_timeout", 120),
                    stream=True,
                    headers={'User-Agent': 'Google-Cloud-Function-Scraper/1.0'}
                )
                pdf_response.raise_for_status()
                
                # Upload to GCS
                pdf_blob = storage_client.bucket(gcs_bucket_name).blob(current_gcs_pdf_path)
                pdf_blob.upload_from_file(pdf_response.raw, content_type='application/pdf')
                
                gcs_uri = f"gs://{gcs_bucket_name}/{current_gcs_pdf_path}"
                pdf_gcs_paths.append(gcs_uri)
                
                active_logger.info(f"Successfully downloaded direct PDF: {main_url} -> {gcs_uri}")
                
            except Exception as e:
                active_logger.error(f"Failed to download direct PDF {main_url}: {e}")
                # Update with error status
                doc_ref.update({
                    "processing_status": "error_pdf_download",
                    "last_error": str(e),
                    "last_updated": firestore.SERVER_TIMESTAMP
                })
                retry_payload = {
                    "customer": customer_id, "project": project_id_config_name, "identifier": identifier,
                    "main_url": main_url, "error_message": str(e),
                    "stage": "fetch_content_pdf_download",
                    "original_pubsub_message": pubsub_message
                }
                publish_to_retry_topic(publisher_client, gcp_project_id, retry_payload, active_logger)
                return {"status": "error_sent_to_retry", "message": f"PDF download failed: {e}"}
            
            # Update Firestore with PDF path
            firestore_update_data = {
                "pdf_gcs_paths": pdf_gcs_paths,
                "processing_status": "content_fetched",
                "content_fetched_timestamp": firestore.SERVER_TIMESTAMP,
                "last_updated": firestore.SERVER_TIMESTAMP,
                "last_error": firestore.DELETE_FIELD
            }
            
            for attempt in range(MAX_FIRESTORE_RETRIES):
                try:
                    doc_ref.update(firestore_update_data)
                    active_logger.info(f"Successfully updated Firestore for PDF document {identifier}")
                    break
                except Exception as e_fs:
                    active_logger.warning(f"Firestore update attempt {attempt + 1}/{MAX_FIRESTORE_RETRIES} for {identifier} failed: {e_fs}")
                    if attempt == MAX_FIRESTORE_RETRIES - 1:
                        active_logger.error(f"Max retries for Firestore update for {identifier}")
                        raise
                    time.sleep(RETRY_BACKOFF * (2 ** attempt))
            
            # Trigger XML generation
            next_step_payload = {
                "customer": customer_id,
                "project": project_id_config_name,
                "identifier": identifier,
                "date": default_date_str
            }
            topic_path_next_step = publisher_client.topic_path(gcp_project_id, NEXT_STEP_GENERATE_XML_TOPIC_NAME)
            future = publisher_client.publish(topic_path_next_step, json.dumps(next_step_payload).encode("utf-8"))
            future.result(timeout=60)
            active_logger.info(f"Published PDF document to XML generation: {identifier}")
            
            return {"status": "success", "identifier": identifier, "pdf_downloaded": True}
        
        elif document_type == "STRUCTURED_DATA":
            # No content to fetch for structured data entries
            active_logger.info(f"No content to fetch for structured data: {doc_data.get('Document_Title')}")
            
            doc_ref.update({
                "processing_status": "no_content_to_fetch",
                "content_fetched_timestamp": firestore.SERVER_TIMESTAMP,
                "last_updated": firestore.SERVER_TIMESTAMP
            })
            
            # Trigger XML generation
            next_step_payload = {
                "customer": customer_id,
                "project": project_id_config_name,
                "identifier": identifier,
                "date": default_date_str
            }
            topic_path_next_step = publisher_client.topic_path(gcp_project_id, NEXT_STEP_GENERATE_XML_TOPIC_NAME)
            future = publisher_client.publish(topic_path_next_step, json.dumps(next_step_payload).encode("utf-8"))
            future.result(timeout=60)
            active_logger.info(f"Published structured data to XML generation: {identifier}")
            
            return {"status": "success", "identifier": identifier, "no_content_needed": True}
        
        else:  # HTML_DOCUMENT - enhanced existing logic
            active_logger.info(f"Processing HTML document: {main_url}")
            
            # --- 1. Fetch HTML content ---
            html_content = None
            try:
                scrape_timeout = project_config.get("fetch_content_scrape_timeout", 60) 
                response = requests.get(main_url, timeout=scrape_timeout, headers={'User-Agent': 'Google-Cloud-Function-Scraper/1.0'})
                response.raise_for_status()
                try:
                    html_content = response.content.decode(response.encoding if response.encoding else 'utf-8', errors='replace')
                except (LookupError, TypeError): 
                     html_content = response.text 
                active_logger.info(f"Successfully fetched HTML for {main_url}")
            except requests.exceptions.RequestException as e:
                error_message = f"Failed to fetch HTML for {main_url}: {str(e)}"
                active_logger.error(error_message, exc_info=True)
                if doc_ref:
                     doc_ref.update({"processing_status": "error_content_fetch_html", "last_error": str(e), "last_updated": firestore.SERVER_TIMESTAMP})
                retry_payload = {
                    "customer": customer_id, "project": project_id_config_name, "identifier": identifier,
                    "main_url": main_url, "error_message": str(e),
                    "stage": "fetch_content_html_download",
                    "original_pubsub_message": pubsub_message 
                }
                publish_to_retry_topic(publisher_client, gcp_project_id, retry_payload, active_logger)
                return {"status": "error_sent_to_retry", "message": error_message}

            if not html_content or not html_content.strip():
                active_logger.warning(f"No HTML content retrieved or content is empty for {main_url}.")
                if doc_ref:
                    doc_ref.update({"processing_status": "error_content_empty_html", "last_error": "No HTML content", "last_updated": firestore.SERVER_TIMESTAMP})
                return {"status": "success_processed_empty_html", "message": "No HTML content to process."}

            # --- 2. URL Absolutization ---
            soup = BeautifulSoup(html_content, 'html.parser')
            tags_and_attributes = {
                'img': 'src', 'link': 'href', 'a': 'href', 'script': 'src',
                'iframe': 'src', 'source': 'src', 'track': 'src',
            }
            images_fixed_count = 0
            image_url_fix_config = project_config.get("image_url_fix", {})
            use_regex_fix = image_url_fix_config.get("use_regex_fix", False)
            base_url_for_fix = image_url_fix_config.get("base_url", main_url) 
            regex_pattern_str = image_url_fix_config.get("regex_pattern")
            regex_replacement_str = image_url_fix_config.get("regex_replacement")

            active_logger.info(f"Starting URL absolutization for {main_url}. Strategy: {'regex' if use_regex_fix and regex_pattern_str else 'urljoin'}.")
            for tag_name, attr_name in tags_and_attributes.items():
                for tag_element in soup.find_all(tag_name):
                    if tag_element.has_attr(attr_name):
                        original_url = tag_element[attr_name]
                        if original_url and not original_url.startswith(('http://', 'https://', 'data:', '#', 'mailto:', 'tel:')):
                            absolute_url_val = None
                            if use_regex_fix and regex_pattern_str and regex_replacement_str:
                                absolute_url_val = urljoin(base_url_for_fix, original_url) 
                            else:
                                absolute_url_val = urljoin(main_url, original_url) 

                            if absolute_url_val:
                                tag_element[attr_name] = absolute_url_val
                                if tag_name == 'img': images_fixed_count +=1
            active_logger.info(f"Absolutized URLs. Images fixed/updated: {images_fixed_count}")
            processed_html_content = soup.prettify(formatter="html5")

            # --- 3. ENHANCED PDF Discovery - Smart attachment detection ---
            pdf_gcs_paths = []
            pdf_attachment_count = 0
            
            # Find all PDF links
            pdf_links = []
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                if isinstance(href, str):
                    parsed_href = urlparse(href)
                    if parsed_href.scheme and parsed_href.netloc and href.lower().endswith('.pdf'):
                        # NEW: Only download PDFs that appear to be meaningful attachments
                        if is_meaningful_pdf_attachment(a_tag, href):
                            pdf_links.append({
                                'url': href,
                                'element': a_tag,
                                'text': a_tag.get_text(strip=True)
                            })
                            active_logger.debug(f"Found meaningful PDF attachment: {a_tag.get_text(strip=True)} -> {href}")
                        else:
                            active_logger.debug(f"Skipped PDF (navigation/generic): {a_tag.get_text(strip=True)} -> {href}")
                else:
                    active_logger.warning(f"Found non-string href in <a> tag: {href} on page {main_url}")
            
            active_logger.info(f"Found {len(pdf_links)} meaningful PDF attachments for {main_url}.")

            # Download meaningful PDF attachments
            for pdf_link in pdf_links:
                pdf_url = pdf_link['url']
                pdf_text = pdf_link['text']
                pdf_attachment_count += 1
                
                try:
                    # Use indexed filename + attachment suffix
                    pdf_filename_suffix_for_template = f"attachment_{pdf_attachment_count}"
                    
                    current_gcs_pdf_path = gcs_pdf_path_template.format(
                        project=project_id_config_name,
                        identifier=identifier,
                        indexed_filename_base=indexed_filename_base,
                        pdf_filename_suffix=pdf_filename_suffix_for_template,
                        date_str=default_date_str
                    )
                    pdf_blob = storage_client.bucket(gcs_bucket_name).blob(current_gcs_pdf_path)

                    for attempt in range(MAX_PDF_DOWNLOAD_RETRIES):
                        try:
                            # Better approach - download complete content first
                            pdf_response = requests.get(pdf_url, timeout=120, headers={'User-Agent': 'Google-Cloud-Function-Scraper/1.0'})
                            pdf_response.raise_for_status()
                            pdf_blob.upload_from_string(pdf_response.content, content_type='application/pdf')
                            gcs_uri = f"gs://{gcs_bucket_name}/{current_gcs_pdf_path}"
                            pdf_gcs_paths.append(gcs_uri)
                            active_logger.info(f"Downloaded PDF attachment: '{pdf_text}' from {pdf_url} to {gcs_uri}")
                            break 
                        except requests.exceptions.RequestException as e_pdf_req:
                            active_logger.warning(f"PDF download attempt {attempt + 1}/{MAX_PDF_DOWNLOAD_RETRIES} for {pdf_url} failed: {e_pdf_req}")
                            if attempt == MAX_PDF_DOWNLOAD_RETRIES - 1:
                                active_logger.error(f"Max retries reached for PDF download {pdf_url}. Skipping this PDF.")
                            else:
                                time.sleep(RETRY_BACKOFF * (2 ** attempt))
                        except Exception as e_gcs_pdf_upload: 
                            active_logger.warning(f"GCS PDF upload attempt {attempt + 1}/{MAX_PDF_DOWNLOAD_RETRIES} for {pdf_url} to {current_gcs_pdf_path} failed: {e_gcs_pdf_upload}")
                            if attempt == MAX_PDF_DOWNLOAD_RETRIES - 1:
                                active_logger.error(f"Max retries for GCS PDF upload {pdf_url}. Skipping this PDF.")
                            else:
                                time.sleep(RETRY_BACKOFF * (2 ** attempt))
                except Exception as e_outer_pdf_loop:
                     active_logger.error(f"Outer error processing PDF URL {pdf_url}: {e_outer_pdf_loop}", exc_info=True)

            active_logger.info(f"Downloaded {len(pdf_gcs_paths)} PDF attachments from HTML document {main_url}")

            # --- 4. Store Processed HTML to GCS ---
            final_gcs_html_path = gcs_html_path_template.format(
                project=project_id_config_name,
                identifier=identifier,
                indexed_filename_base=indexed_filename_base,
                date_str=default_date_str
            )

            html_blob = storage_client.bucket(gcs_bucket_name).blob(final_gcs_html_path)
            uploaded_html_gcs_uri = None

            for attempt in range(MAX_GCS_RETRIES):
                try:
                    html_blob.upload_from_string(processed_html_content, content_type='text/html; charset=utf-8')
                    uploaded_html_gcs_uri = f"gs://{gcs_bucket_name}/{final_gcs_html_path}"
                    active_logger.info(f"Stored processed HTML for {identifier} to {uploaded_html_gcs_uri} (using base: {indexed_filename_base})")
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
                active_logger.error(f"HTML GCS path was not set for {identifier}, likely due to upload failure.")
                return {"status": "error_internal", "message": "HTML GCS URI not set after retries."}

            # --- 5. Update Firestore document ---
            firestore_update_data = {
                "html_gcs_path": uploaded_html_gcs_uri,
                "pdf_gcs_paths": pdf_gcs_paths,
                "processing_status": "content_fetched",
                "content_fetched_timestamp": firestore.SERVER_TIMESTAMP,
                "images_fixed_count": images_fixed_count,
                "pdf_attachments_count": len(pdf_gcs_paths),
                "last_updated": firestore.SERVER_TIMESTAMP,
                "last_error": firestore.DELETE_FIELD 
            }

            for attempt in range(MAX_FIRESTORE_RETRIES):
                try:
                    doc_ref.update(firestore_update_data)
                    active_logger.info(f"Successfully updated Firestore for HTML document {identifier} with content paths.")
                    break
                except Exception as e_fs:
                    active_logger.warning(f"Firestore update attempt {attempt + 1}/{MAX_FIRESTORE_RETRIES} for {identifier} failed: {e_fs}")
                    if attempt == MAX_FIRESTORE_RETRIES - 1:
                        active_logger.error(f"Max retries for Firestore update for {identifier}. State may be inconsistent.")
                        retry_payload = {
                            "customer": customer_id, "project": project_id_config_name, "identifier": identifier,
                            "main_url": main_url, "error_message": str(e_fs),
                            "stage": "fetch_content_firestore_update",
                            "item_data_snapshot": {**doc_data, **firestore_update_data}, 
                            "original_pubsub_message": pubsub_message
                        }
                        publish_to_retry_topic(publisher_client, gcp_project_id, retry_payload, active_logger)
                        return {"status": "error_sent_to_retry", "message": f"Firestore update failed for {identifier}"}
                    time.sleep(RETRY_BACKOFF * (2 ** attempt))

            # --- 6. Publish to next step (generate-xml) ---
            next_step_payload = {
                "customer": customer_id,
                "project": project_id_config_name,
                "identifier": identifier,
                "date": default_date_str 
            }
            topic_path_next_step = publisher_client.topic_path(gcp_project_id, NEXT_STEP_GENERATE_XML_TOPIC_NAME)
            future = publisher_client.publish(topic_path_next_step, json.dumps(next_step_payload).encode("utf-8"))
            future.result(timeout=60) 
            active_logger.info(f"Published HTML document to XML generation: {identifier}")

            return {"status": "success", "identifier": identifier, "html_gcs_path": uploaded_html_gcs_uri, "html_processed": True}

    except Exception as e:
        current_logger = active_logger if active_logger != logger else logger
        error_message_critical = f"Critical error in fetch_content for identifier '{identifier}', main_url '{main_url}', pubsub_message: '{str(pubsub_message)[:500]}': {str(e)}"
        current_logger.error(error_message_critical, exc_info=True)
        
        if doc_ref: 
            try:
                doc_ref.update({"processing_status": "error_content_critical", "last_error": str(e), "last_updated": firestore.SERVER_TIMESTAMP})
            except Exception as e_fs_crit:
                current_logger.error(f"Failed to update Firestore with critical error details for {identifier}: {e_fs_crit}")
        
        if publisher_client is None and 'gcp_project_id' in locals() and gcp_project_id:
            try:
                publisher_client = pubsub_v1.PublisherClient()
            except Exception as e_pub_init:
                current_logger.error(f"Failed to initialize PublisherClient for critical error retry: {e_pub_init}")
        
        if publisher_client and 'gcp_project_id' in locals() and gcp_project_id:
            retry_payload_critical = {
                "customer": customer_id, 
                "project": project_id_config_name, 
                "identifier": identifier, 
                "main_url": pubsub_message.get("main_url", "N/A_in_critical_error"),
                "error_message": str(e),
                "stage": "fetch_content_critical",
                "original_pubsub_message": pubsub_message
            }
            publish_to_retry_topic(publisher_client, gcp_project_id, retry_payload_critical, current_logger)
        else:
            current_logger.error("Cannot publish critical error to retry topic: publisher_client or GCP Project ID not available.")
        raise