import json
import base64
import logging
import re
import os
import time
import gzip
from google.cloud import firestore, pubsub_v1, storage
from src.common.utils import setup_logging
from src.common.config import load_customer_config, load_project_config, get_secret
from src.common.helpers import get_mapped_field, sanitize_error_message
from datetime import datetime
import functions_framework
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom

logger = logging.getLogger(__name__)
RETRY_TOPIC_NAME = "retry-pipeline"
NEXT_STEP_GENERATE_REPORTS_TOPIC_NAME = "generate-reports"
MAX_GCS_RETRIES = 3
MAX_FIRESTORE_RETRIES = 3
RETRY_BACKOFF = 5

def serialize_firestore_doc(data):
    """Convert Firestore document data to JSON-serializable format."""
    if isinstance(data, dict):
        return {k: serialize_firestore_doc(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [serialize_firestore_doc(item) for item in data]
    elif isinstance(data, datetime):
        return data.isoformat()
    return data

def sanitize_filename(filename_base, identifier, use_identifier_only=False):
    """Sanitize filename for XML storage, using identifier only for GCS file name."""
    if use_identifier_only:
        sanitized_title = re.sub(r'[^\w\-_.]', '_', identifier).strip('._')[:150]
        return f"{sanitized_title}.xml"
    if not filename_base or not isinstance(filename_base, str) or filename_base == "Not Available":
        filename_base = f"document_{identifier}"
    sanitized_title = re.sub(r'[^\w\-_.]', '_', filename_base).replace(' ', '_').strip('._')[:150]
    return f"{sanitized_title}.xml" if sanitized_title else f"document_{identifier}.xml"

def validate_html_content(content, logger_instance):
    """Validate and normalize HTML content encoding."""
    if not content:
        return None
    try:
        # Ensure content is a string
        if isinstance(content, bytes):
            try:
                content = content.decode('utf-8')
            except UnicodeDecodeError:
                try:
                    content = content.decode('latin-1')
                    logger_instance.warning("HTML content decoded with latin-1 fallback.")
                except UnicodeDecodeError:
                    content = content.decode('utf-8', errors='replace')
                    logger_instance.warning("HTML content decoded with error replacement.")
        # Verify UTF-8 compatibility
        content.encode('utf-8')
        return content
    except Exception as e:
        logger_instance.error(f"Failed to validate HTML content: {str(e)}")
        return None

def custom_pretty_xml(element, xml_structure_config):
    """Format XML with proper indentation and CDATA sections."""
    rough_string = tostring(element, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    pretty_xml_intermediate = reparsed.toprettyxml(indent=xml_structure_config.get('indent', '  '))
    final_xml_string = pretty_xml_intermediate
    for field_config in xml_structure_config.get('fields', []):
        if field_config.get('cdata', False):
            tag = field_config['tag']
            final_xml_string = re.sub(
                rf'(<{tag}>)(.*?)(</{tag}>)',
                lambda m: f'{m.group(1)}<![CDATA[{m.group(2).strip()}]]>{m.group(3)}',
                final_xml_string,
                flags=re.DOTALL
            )
    if xml_structure_config.get('declaration', True):
        if not final_xml_string.strip().startswith('<?xml'):
            final_xml_string = '<?xml version="1.0" encoding="UTF-8"?>\n' + final_xml_string
        else:
            final_xml_string = re.sub(r'<\?xml.*?\?>', '<?xml version="1.0" encoding="UTF-8"?>', final_xml_string, count=1, flags=re.IGNORECASE)
    else:
        final_xml_string = re.sub(r'<\?xml.*?\?>\n?', '', final_xml_string, count=1, flags=re.IGNORECASE).lstrip()
    return final_xml_string.encode('utf-8')

@functions_framework.cloud_event
def generate_xml(cloud_event):
    active_logger = logger
    data = {}
    identifier = "unknown_identifier"
    gcp_project_id_for_error = os.environ.get("GCP_PROJECT")
    publisher_for_error = None

    try:
        pubsub_data_encoded = cloud_event.data.get("message", {}).get("data")
        if not pubsub_data_encoded:
            raise ValueError("No data in Pub/Sub message")

        data = json.loads(base64.b64decode(pubsub_data_encoded).decode('utf-8'))
        customer_id = data.get('customer')
        project_id = data.get('project')
        dataset_id = data.get('dataset_id', 'unknown_dataset')
        dataset_type = data.get('dataset_type', 'items')
        identifier = data.get('identifier')
        date = data.get('date', datetime.now().strftime('%Y%m%d'))

        missing_fields = [f for f, v in {"customer": customer_id, "project": project_id, "identifier": identifier}.items() if not v]
        if missing_fields:
            raise ValueError(f"Missing required Pub/Sub fields: {', '.join(missing_fields)}")

        active_logger = setup_logging(customer_id, project_id)
        active_logger.info(f"Starting generate_xml for identifier: {identifier}, dataset_id: {dataset_id}, date: {date}")

        customer_config = load_customer_config(customer_id)
        project_config = load_project_config(project_id)
        gcp_project_id = customer_config.get("gcp_project_id", os.environ.get("GCP_PROJECT"))
        gcp_project_id_for_error = gcp_project_id
        if not gcp_project_id:
            raise ValueError("GCP Project ID not configured")

        bucket_name = project_config.get('gcs_bucket')
        firestore_collection_name = project_config.get('firestore_collection')
        firestore_db_id = project_config.get('firestore_database_id', '(default)')
        xml_structure_config = project_config.get('xml_structure')
        field_mappings = project_config.get('field_mappings', {})
        error_collection_name = f"{firestore_collection_name}_errors"

        if not all([bucket_name, firestore_collection_name, xml_structure_config]):
            raise ValueError("Missing config: gcs_bucket, firestore_collection, or xml_structure.")

        if dataset_type == 'search_results':
            active_logger.info(f"Skipping XML generation for dataset_type 'search_results' for {identifier}.")
            return {'status': 'skipped_search_results', 'identifier': identifier}

        storage_client = storage.Client(project=gcp_project_id)
        bucket = storage_client.bucket(bucket_name)
        db_options = {"project": gcp_project_id}
        if firestore_db_id != "(default)":
            db_options["database"] = firestore_db_id
        db = firestore.Client(**db_options)
        publisher_for_error = publisher = pubsub_v1.PublisherClient()

        doc_ref = db.collection(firestore_collection_name).document(identifier)
        doc = doc_ref.get()

        if not doc.exists:
            active_logger.error(f"Firestore document {identifier} not found in {firestore_collection_name}.")
            raise ValueError(f"Firestore document {identifier} not found.")

        doc_data = doc.to_dict()
        html_content = validate_html_content(doc_data.get('fixed_html_content_in_fs'), active_logger)

        # Fallback to GCS if fixed_html_content_in_fs is missing or invalid
        if not html_content:
            active_logger.warning(f"No valid fixed_html_content_in_fs for {identifier}. Falling back to GCS html_path.")
            html_path_final_gcs = doc_data.get('html_path')
            if not html_path_final_gcs or not html_path_final_gcs.startswith(f"gs://{bucket_name}/"):
                error_msg = f"Invalid or missing final GCS html_path for {identifier}: {html_path_final_gcs}"
                active_logger.error(error_msg)
                doc_ref.update({"xml_status": "Error: Missing valid final HTML GCS path", "last_updated": firestore.SERVER_TIMESTAMP})
                raise ValueError(error_msg)

            if "/temp_html/" in html_path_final_gcs:
                error_msg = f"HTML path {html_path_final_gcs} for {identifier} appears to be temporary."
                active_logger.error(error_msg)
                doc_ref.update({"xml_status": f"Error: {error_msg}", "last_updated": firestore.SERVER_TIMESTAMP})
                raise ValueError(error_msg)

            try:
                blob_name_final = html_path_final_gcs.replace(f"gs://{bucket_name}/", "")
                blob_final = bucket.blob(blob_name_final)
                if not blob_final.exists():
                    raise FileNotFoundError(f"Final HTML blob {blob_name_final} not found in GCS.")
                gzipped_content_final = blob_final.download_as_bytes()
                html_content = validate_html_content(gzip.decompress(gzipped_content_final), active_logger)
                if not html_content:
                    raise ValueError("Invalid HTML content after decompression.")
                active_logger.info(f"Successfully downloaded and decompressed final HTML for {identifier} from {html_path_final_gcs}")
            except Exception as e_gcs_final_download:
                active_logger.error(f"Failed to download/decompress final HTML from GCS for {identifier} ({html_path_final_gcs}): {e_gcs_final_download}", exc_info=True)
                doc_ref.update({"xml_status": f"Error: Failed GCS HTML download {str(e_gcs_final_download)[:200]}", "last_updated": firestore.SERVER_TIMESTAMP})
                raise

        active_logger.info(f"Using HTML content for {identifier} (source: {'fixed_html_content_in_fs' if doc_data.get('fixed_html_content_in_fs') else 'GCS'})")

        try:
            root = Element(xml_structure_config.get('root_tag', 'GermanyFederalLaw'))
            for field_config in xml_structure_config.get('fields', []):
                tag_name = field_config['tag']
                source_field_name = field_config['source']
                field_value_str = "Not Available"

                if source_field_name == "fixed_html_content":
                    field_value_str = html_content if html_content else "Not Available"
                else:
                    # Use get_mapped_field to resolve field value dynamically
                    temp_value = get_mapped_field(doc_data, source_field_name, field_mappings, logger_instance=active_logger)
                    if temp_value is not None:
                        field_value_str = str(temp_value)
                    else:
                        field_value_str = str(doc_data.get(source_field_name, "Not Available"))

                sub_element = SubElement(root, tag_name)
                sub_element.text = field_value_str

            xml_bytes = custom_pretty_xml(root, xml_structure_config)
        except Exception as e_xml_build:
            active_logger.error(f"Failed to build XML for {identifier}: {e_xml_build}", exc_info=True)
            for attempt in range(MAX_FIRESTORE_RETRIES):
                try:
                    doc_ref.update({"xml_status": f"Error: XML build failed {str(e_xml_build)[:200]}", "last_updated": firestore.SERVER_TIMESTAMP})
                    break
                except Exception as e_update:
                    active_logger.warning(f"Firestore update failed on attempt {attempt + 1}: {str(e_update)}")
                    if attempt < MAX_FIRESTORE_RETRIES - 1:
                        time.sleep(RETRY_BACKOFF * (2 ** attempt))
                    else:
                        active_logger.error(f"Max retries reached for Firestore update: {str(e_update)}")
                        raise
            raise

        # Determine XML filename using filename_field or Law-ID and Abbreviation
        filename_field = xml_structure_config.get('filename_field', 'fullname')
        filename_value = get_mapped_field(doc_data, filename_field, field_mappings, logger_instance=active_logger)
        if filename_value and filename_value != "Not Available":
            xml_filename_base = filename_value
        else:
            law_id = doc_data.get("Law-ID", "Unknown_LawID")
            abbreviation = doc_data.get("Abbreviation", "Unknown_Abbreviation")
            active_logger.debug(f"Law-ID: {law_id}, Abbreviation: {abbreviation} for identifier: {identifier}")
            if law_id == "Unknown_LawID" or abbreviation == "Unknown_Abbreviation":
                active_logger.warning(f"Missing Law-ID or Abbreviation for {identifier}. Using identifier as fallback.")
                xml_filename_base = f"document_{identifier}"
            else:
                xml_filename_base = f"{law_id}_{abbreviation}"
        xml_filename_sanitized = sanitize_filename(xml_filename_base, identifier, use_identifier_only=False)
        active_logger.info(f"Generated filename: {xml_filename_sanitized}")

        xml_gcs_output_path_template = xml_structure_config.get('gcs_xml_path', f"delivered_xml/{project_id}/<date>")
        final_xml_gcs_path_template = xml_gcs_output_path_template.replace("<project_id>", "".join(c if c.isalnum() else '_' for c in project_id)).replace("<date>", date)
        xml_destination_gcs_path = f"{final_xml_gcs_path_template.rstrip('/')}/{sanitize_filename(identifier, identifier, use_identifier_only=True)}"

        xml_blob_final = bucket.blob(xml_destination_gcs_path)
        try:
            for attempt in range(MAX_GCS_RETRIES):
                try:
                    xml_blob_final.upload_from_string(xml_bytes, content_type='application/xml; charset=utf-8')
                    active_logger.info(f"Saved final XML for {identifier} to gs://{bucket_name}/{xml_destination_gcs_path}")
                    break
                except Exception as e_upload:
                    active_logger.warning(f"GCS upload failed for {xml_destination_gcs_path} on attempt {attempt + 1}: {str(e_upload)}")
                    if attempt < MAX_GCS_RETRIES - 1:
                        time.sleep(RETRY_BACKOFF * (2 ** attempt))
                    else:
                        raise Exception(f"Max retries reached for GCS upload: {str(e_upload)}")
        except Exception as e_xml_upload:
            active_logger.error(f"Failed to upload final XML for {identifier}: {e_xml_upload}", exc_info=True)
            for attempt in range(MAX_FIRESTORE_RETRIES):
                try:
                    doc_ref.update({"xml_status": f"Error: XML GCS upload failed {str(e_xml_upload)[:200]}", "last_updated": firestore.SERVER_TIMESTAMP})
                    break
                except Exception as e_update:
                    active_logger.warning(f"Firestore update failed on attempt {attempt + 1}: {str(e_update)}")
                    if attempt < MAX_FIRESTORE_RETRIES - 1:
                        time.sleep(RETRY_BACKOFF * (2 ** attempt))
                    else:
                        active_logger.error(f"Max retries reached for Firestore update: {str(e_update)}")
                        raise
            raise

        # Update Firestore with XML metadata, with retry logic
        for attempt in range(MAX_FIRESTORE_RETRIES):
            try:
                doc_ref.update({
                    'xml_path': f"gs://{bucket_name}/{xml_destination_gcs_path}",
                    'xml_filename': xml_filename_sanitized,
                    'xml_status': 'Success',
                    'xml_generated_at': firestore.SERVER_TIMESTAMP,
                    'processing_stage': 'xml_generated',
                    'last_updated': firestore.SERVER_TIMESTAMP
                })
                active_logger.info(f"Successfully updated Firestore for {identifier} with XML path and status.")
                break
            except Exception as e_update:
                active_logger.warning(f"Firestore update failed on attempt {attempt + 1}: {str(e_update)}")
                if attempt < MAX_FIRESTORE_RETRIES - 1:
                    time.sleep(RETRY_BACKOFF * (2 ** attempt))
                else:
                    active_logger.error(f"Max retries reached for Firestore update: {str(e_update)}")
                    raise

        next_step_payload = {
            'customer': customer_id,
            'project': project_id,
            'dataset_id': dataset_id,
            'dataset_type': dataset_type,
            'date': date,
            'status': 'xml_generation_completed_for_item',
            'identifier': identifier,
            'xml_output_gcs_prefix': final_xml_gcs_path_template
        }
        publisher.publish(
            publisher.topic_path(gcp_project_id, NEXT_STEP_GENERATE_REPORTS_TOPIC_NAME),
            json.dumps(next_step_payload).encode('utf-8')
        )
        active_logger.info(f"Published to '{NEXT_STEP_GENERATE_REPORTS_TOPIC_NAME}' for {identifier}")

        return {'status': 'success', 'identifier': identifier, 'xml_path': f"gs://{bucket_name}/{xml_destination_gcs_path}"}

    except Exception as e_critical:
        active_logger.error(f"Critical error in generate_xml for identifier '{identifier}': {str(e_critical)}", exc_info=True)
        if 'data' in locals() and data.get("customer") and data.get("project"):
            gcp_project_id_to_use = gcp_project_id if 'gcp_project_id' in locals() and gcp_project_id else gcp_project_id_for_error
            if gcp_project_id_to_use:
                retry_payload = {
                    "customer": data.get("customer"),
                    "project": data.get("project"),
                    "original_pubsub_message": data,
                    "error_message": sanitize_error_message(str(e_critical)),
                    "stage": "generate_xml_critical",
                    "retry_count": data.get("retry_count", 0) + 1,
                    "identifier_in_error": identifier,
                    "doc_data": serialize_firestore_doc(doc_data) if 'doc_data' in locals() else None
                }
                if publisher_for_error is None:
                    publisher_for_error = pubsub_v1.PublisherClient()
                publisher_for_error.publish(
                    publisher_for_error.topic_path(gcp_project_id_to_use, RETRY_TOPIC_NAME),
                    json.dumps(retry_payload, default=serialize_firestore_doc).encode('utf-8')
                )
                active_logger.info("Published critical error from generate_xml to retry topic.")
            else:
                active_logger.error("GCP Project ID for error reporting not found in generate_xml.")
        raise