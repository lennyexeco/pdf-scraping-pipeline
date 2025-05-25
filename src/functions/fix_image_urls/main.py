import json
import base64
import logging
import re
import os
import time
from google.cloud import firestore, pubsub_v1, storage
from src.common.utils import setup_logging
from src.common.config import load_customer_config, load_project_config, get_secret
from src.common.helpers import get_mapped_field, sanitize_error_message
from datetime import datetime
import functions_framework

logger = logging.getLogger(__name__)
RETRY_TOPIC_NAME = "retry-pipeline"
NEXT_STEP_STORE_HTML_TOPIC_NAME = "store-html"
MAX_GCS_RETRIES = 3
MAX_FIRESTORE_RETRIES = 3
RETRY_BACKOFF = 2

def serialize_firestore_doc(data):
    """Convert Firestore document data to JSON-serializable format."""
    if isinstance(data, dict):
        return {k: serialize_firestore_doc(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [serialize_firestore_doc(item) for item in data]
    elif isinstance(data, datetime):
        return data.isoformat()
    return data

def sanitize_filename(filename_base, identifier, logger_instance):
    """Sanitize filename for GCS storage."""
    if not filename_base or not isinstance(filename_base, str) or filename_base == "Not Available":
        logger_instance.warning(f"No valid filename_base for {identifier}, using identifier.")
        filename_base = f"document_{identifier}"
    sanitized_title = re.sub(r'[^\w\-_.]', '_', filename_base).replace(' ', '_').strip('._')[:150]
    return sanitized_title if sanitized_title else f"document_{identifier}"

@functions_framework.cloud_event
def fix_image_urls(cloud_event):
    active_logger = logger
    data = {}
    identifier = "unknown_identifier"
    publisher_client = None

    try:
        pubsub_data_encoded = cloud_event.data.get("message", {}).get("data")
        if not pubsub_data_encoded:
            active_logger.error("No 'data' in Pub/Sub message envelope.")
            raise ValueError("No 'data' in Pub/Sub message envelope.")

        data = json.loads(base64.b64decode(pubsub_data_encoded).decode('utf-8'))
        customer_id = data.get('customer')
        project_id = data.get('project')
        identifier = data.get('identifier')
        main_url = data.get('main_url') or data.get('mainUrl')
        html_path_gcs_temp = data.get('html_path')
        date_str = data.get('date', datetime.now().strftime('%Y%m%d'))
        apify_dataset_id_source = data.get('apify_dataset_id_source')
        apify_run_id_trigger = data.get('apify_run_id_trigger')
        apify_search_results_dataset_id = data.get('apify_search_results_dataset_id')

        if not all([customer_id, project_id, identifier, main_url, html_path_gcs_temp]):
            missing_fields_list = [
                f_name for f_name, f_val in {
                    "customer": customer_id, "project": project_id,
                    "identifier": identifier, "main_url": main_url,
                    "html_path": html_path_gcs_temp
                }.items() if not f_val
            ]
            active_logger.error(f"Missing required fields from PubSub: {', '.join(missing_fields_list)}. Data: {str(data)[:500]}")
            raise ValueError(f"Missing required fields: {', '.join(missing_fields_list)}")

        active_logger = setup_logging(customer_id, project_id)
        active_logger.info(f"Processing fix_image_urls for identifier: {identifier} (URL: {main_url}) from GCS path: {html_path_gcs_temp}")

        customer_config = load_customer_config(customer_id)
        project_config = load_project_config(project_id)
        gcp_project_id = customer_config.get('gcp_project_id', os.environ.get("GCP_PROJECT"))
        if not gcp_project_id:
            active_logger.error("GCP Project ID not configured.")
            raise ValueError("GCP Project ID not configured")

        firestore_collection_name = project_config.get('firestore_collection')
        error_collection_name = f"{firestore_collection_name}_errors"
        image_url_fix_config = project_config.get('image_url_fix')
        gcs_bucket_name = project_config.get("gcs_bucket")
        field_mappings = project_config.get('field_mappings', {})
        xml_structure_config = project_config.get('xml_structure', {})
        apify_search_results_dataset_id_default = project_config.get('apify_search_results_dataset_id')

        if not all([firestore_collection_name, image_url_fix_config, gcs_bucket_name]):
            active_logger.error("Missing configuration: firestore_collection, image_url_fix, or gcs_bucket.")
            raise ValueError("Missing critical configuration.")

        db_options = {"project": gcp_project_id}
        firestore_db_id = project_config.get('firestore_database_id', '(default)')
        if firestore_db_id != "(default)":
            db_options["database"] = firestore_db_id
        db = firestore.Client(**db_options)
        storage_client = storage.Client(project=gcp_project_id)
        publisher_client = pubsub_v1.PublisherClient()

        doc_ref = db.collection(firestore_collection_name).document(identifier)
        doc = None
        for attempt in range(MAX_FIRESTORE_RETRIES):
            try:
                doc = doc_ref.get()
                if doc.exists:
                    break
                active_logger.warning(f"Firestore doc {identifier} not found on attempt {attempt + 1}. Retrying...")
                time.sleep(RETRY_BACKOFF * (2 ** attempt))
            except Exception as e:
                active_logger.warning(f"Firestore fetch failed on attempt {attempt + 1}: {str(e)}")
                if attempt < MAX_FIRESTORE_RETRIES - 1:
                    time.sleep(RETRY_BACKOFF * (2 ** attempt))
                else:
                    raise ValueError(f"Failed to fetch Firestore document after retries: {str(e)}")

        if not doc or not doc.exists:
            active_logger.error(f"No Firestore document found for identifier {identifier} after retries.")
            return {'status': 'error', 'message': 'Firestore document not found after retries.'}

        doc_data = doc.to_dict()

        html_content = None
        try:
            if not html_path_gcs_temp.startswith(f"gs://{gcs_bucket_name}/"):
                raise ValueError(f"GCS path {html_path_gcs_temp} does not match bucket {gcs_bucket_name}")

            blob_path_temp = html_path_gcs_temp.replace(f"gs://{gcs_bucket_name}/", "")
            bucket_obj = storage_client.bucket(gcs_bucket_name)
            blob_temp = bucket_obj.blob(blob_path_temp)

            if not blob_temp.exists():
                raise ValueError(f"Temporary HTML blob {blob_path_temp} not found in GCS.")

            html_content = blob_temp.download_as_text(encoding='utf-8')
            active_logger.info(f"Successfully downloaded HTML for {identifier} from {html_path_gcs_temp}")
        except Exception as e_gcs_download:
            active_logger.error(f"Failed to download HTML from GCS for {identifier} ({html_path_gcs_temp}): {e_gcs_download}", exc_info=True)
            for attempt in range(MAX_FIRESTORE_RETRIES):
                try:
                    doc_ref.update({
                        "processing_stage": "error_fix_images_gcs_temp_download",
                        "last_updated": firestore.SERVER_TIMESTAMP,
                        "error_message": str(e_gcs_download)
                    })
                    break
                except Exception as e_update:
                    if attempt < MAX_FIRESTORE_RETRIES - 1:
                        time.sleep(RETRY_BACKOFF * (2 ** attempt))
                    else:
                        raise
            raise

        if not html_content or not isinstance(html_content, str) or not html_content.strip():
            active_logger.error(f"HTML content from GCS ({html_path_gcs_temp}) is missing or empty for {identifier}.")
            for attempt in range(MAX_FIRESTORE_RETRIES):
                try:
                    doc_ref.update({
                        "processing_stage": "error_fix_images_no_html_from_gcs",
                        "last_updated": firestore.SERVER_TIMESTAMP,
                        "error_message": "Empty HTML from GCS temp path."
                    })
                    break
                except Exception as e_update:
                    if attempt < MAX_FIRESTORE_RETRIES - 1:
                        time.sleep(RETRY_BACKOFF * (2 ** attempt))
                    else:
                        raise
            return {'status': 'error', 'message': 'Empty HTML content from GCS.'}

        base_url_str = image_url_fix_config.get('base_url')
        regex_pattern_str = image_url_fix_config.get('regex_pattern')
        if not base_url_str or not regex_pattern_str:
            raise ValueError("image_url_fix configuration (base_url or regex_pattern) is missing.")

        pattern = re.compile(regex_pattern_str)
        images_fixed_count = 0

        def replacement_func(match):
            nonlocal images_fixed_count
            try:
                prefix_capture, relative_path_capture, suffix_capture = match.groups()
                if base_url_str.endswith('/') and relative_path_capture.startswith('/'):
                    abs_path = base_url_str + relative_path_capture[1:]
                elif not base_url_str.endswith('/') and not relative_path_capture.startswith('/'):
                    abs_path = base_url_str + '/' + relative_path_capture
                else:
                    abs_path = base_url_str + relative_path_capture
                images_fixed_count += 1
                return f"{prefix_capture}{abs_path}{suffix_capture}"
            except IndexError:
                active_logger.warning(f"Regex pattern '{regex_pattern_str}' mismatch for match: {match.group(0)}. Skipping.")
                return match.group(0)

        fixed_html_content = pattern.sub(replacement_func, html_content)
        active_logger.info(f"Fixed {images_fixed_count} image URLs for {identifier}.")

        computed_fields = {}
        for field, mapping in field_mappings.items():
            if mapping.get('type') == 'computed' and 'extract_date_from_html' in mapping.get('source', ''):
                try:
                    value = get_mapped_field(
                        {"htmlContent": fixed_html_content},
                        field,
                        field_mappings,
                        logger_instance=active_logger
                    )
                    if value and value != "Not Available":
                        computed_fields[field] = value
                        active_logger.info(f"Extracted {field}: {computed_fields[field]} for {identifier}")
                except Exception as e_extract:
                    active_logger.error(f"Failed to extract {field} for {identifier}: {str(e_extract)}")

        filename_field = xml_structure_config.get('filename_field', 'fullname')
        filename_value = get_mapped_field(doc_data, filename_field, field_mappings, logger_instance=active_logger)
        if filename_value and filename_value != "Not Available":
            filename_base = filename_value
        else:
            law_id = doc_data.get("Law-ID", "Unknown_LawID")
            abbreviation = doc_data.get("Abbreviation", "Unknown_Abbreviation")
            if law_id != "Unknown_LawID" and abbreviation != "Unknown_Abbreviation":
                filename_base = f"{law_id}_{abbreviation}"
            else:
                filename_base = identifier
        filename_sanitized = sanitize_filename(filename_base, identifier, active_logger)

        safe_project_id_path = "".join(c if c.isalnum() else '_' for c in project_id)
        gcs_fixed_html_path_template = image_url_fix_config.get("gcs_fixed_path", f"fixed/{safe_project_id_path}/<date>")
        final_gcs_fixed_html_path = gcs_fixed_html_path_template.replace("<date>", date_str)
        destination_blob_name_fixed = f"{final_gcs_fixed_html_path.rstrip('/')}/{identifier}_{filename_sanitized}.html"
        gcs_content_type_for_upload = "text/html; charset=utf-8"

        try:
            bucket_obj = storage_client.bucket(gcs_bucket_name)
            blob = bucket_obj.blob(destination_blob_name_fixed)
            for attempt in range(MAX_GCS_RETRIES):
                try:
                    blob.upload_from_string(fixed_html_content, content_type=gcs_content_type_for_upload)
                    gcs_final_html_path = f"gs://{gcs_bucket_name}/{destination_blob_name_fixed}"
                    active_logger.info(f"Saved fixed HTML for {identifier} to GCS: {gcs_final_html_path}")
                    break
                except Exception as e_upload:
                    active_logger.warning(f"GCS upload failed for {destination_blob_name_fixed} on attempt {attempt + 1}: {str(e_upload)}")
                    if attempt < MAX_GCS_RETRIES - 1:
                        time.sleep(RETRY_BACKOFF * (2 ** attempt))
                    else:
                        raise Exception(f"Max retries reached for GCS upload: {str(e_upload)}")
        except Exception as e_gcs_final_upload:
            active_logger.error(f"Failed to upload fixed HTML for {identifier} to GCS: {e_gcs_final_upload}", exc_info=True)
            for attempt in range(MAX_FIRESTORE_RETRIES):
                try:
                    doc_ref.update({
                        "processing_stage": "error_fix_images_gcs_final_upload",
                        "last_updated": firestore.SERVER_TIMESTAMP,
                        "error_message": str(e_gcs_final_upload)
                    })
                    break
                except Exception as e_update:
                    if attempt < MAX_FIRESTORE_RETRIES - 1:
                        time.sleep(RETRY_BACKOFF * (2 ** attempt))
                    else:
                        raise
            raise

        try:
            blob_temp = bucket_obj.blob(blob_path_temp)
            if blob_temp.exists():
                blob_temp.delete()
                active_logger.info(f"Deleted temporary HTML file {html_path_gcs_temp} from GCS for {identifier}.")
        except Exception as e_gcs_delete:
            active_logger.warning(f"Failed to delete temporary GCS file {html_path_gcs_temp} for {identifier}: {e_gcs_delete}")

        update_fields_firestore = {
            "html_path": gcs_final_html_path,
            "images_fixed_count": images_fixed_count,
            "processing_stage": "html_images_fixed_final_gcs",
            "last_updated": firestore.SERVER_TIMESTAMP
        }
        update_fields_firestore.update(computed_fields)

        for attempt in range(MAX_FIRESTORE_RETRIES):
            try:
                doc_ref.update(update_fields_firestore)
                active_logger.info(f"Updated Firestore for {identifier}: final html_path, computed fields, and image count.")
                break
            except Exception as e_update:
                active_logger.warning(f"Firestore update failed on attempt {attempt + 1}: {str(e_update)}")
                if attempt < MAX_FIRESTORE_RETRIES - 1:
                    time.sleep(RETRY_BACKOFF * (2 ** attempt))
                else:
                    active_logger.error(f"Max retries reached for Firestore update: {str(e_update)}")
                    raise

        store_html_payload = {
            "customer": customer_id,
            "project": project_id,
            "dataset_id": apify_search_results_dataset_id or apify_search_results_dataset_id_default,
            "offset": 0,
            "date": date_str,
            "apify_search_results_dataset_id_override": apify_search_results_dataset_id,
            "dataset_type": "items",
            "apify_dataset_id_source": apify_dataset_id_source,
            "apify_run_id_trigger": apify_run_id_trigger
        }
        publisher_client.publish(
            publisher_client.topic_path(gcp_project_id, NEXT_STEP_STORE_HTML_TOPIC_NAME),
            json.dumps(store_html_payload, default=str).encode('utf-8')
        )
        active_logger.info(f"Published to '{NEXT_STEP_STORE_HTML_TOPIC_NAME}' for augmentation after fixing images for {identifier}.")

        return {
            'status': 'success',
            'identifier': identifier,
            'gcs_path_final': gcs_final_html_path,
            'images_fixed': images_fixed_count,
            'computed_fields': computed_fields,
            'message': 'Image URLs fixed, HTML stored in final GCS location, Firestore updated, and sent to store_html for augmentation.'
        }

    except Exception as e_main:
        active_logger.error(f"Critical error in fix_image_urls for identifier '{identifier}': {str(e_main)}", exc_info=True)
        gcp_project_id_fallback = os.environ.get("GCP_PROJECT")
        if 'data' in locals() and data.get('customer') and data.get('project'):
            current_gcp_project_id = gcp_project_id if 'gcp_project_id' in locals() and gcp_project_id else gcp_project_id_fallback
            if current_gcp_project_id:
                try:
                    error_payload_critical = {
                        "identifier": identifier,
                        "error": f"Critical error in fix_image_urls: {str(e_main)}",
                        "stage": "fix_image_urls_critical",
                        "retry_count": data.get("retry_count", 0) + 1,
                        "original_pubsub_data": data,
                        "timestamp": datetime.now().isoformat(),
                        "main_url": data.get("main_url") or data.get("mainUrl"),
                        "customer": data.get("customer"),
                        "project": data.get("project"),
                        "doc_data": serialize_firestore_doc(doc_data) if 'doc_data' in locals() else None
                    }
                    if publisher_client is None:
                        publisher_client = pubsub_v1.PublisherClient()
                    publisher_client.publish(
                        publisher_client.topic_path(current_gcp_project_id, RETRY_TOPIC_NAME),
                        json.dumps(error_payload_critical, default=str).encode('utf-8')
                    )
                    active_logger.info(f"Published critical error for {identifier} from fix_image_urls to retry topic.")
                except Exception as e_log_retry:
                    active_logger.error(f"Failed to publish critical error for {identifier} from fix_image_urls to retry topic: {str(e_log_retry)}")
            else:
                active_logger.error("GCP Project ID for error reporting not found in fix_image_urls.")
        raise