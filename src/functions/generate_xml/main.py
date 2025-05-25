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
from src.common.helpers import get_mapped_field, sanitize_error_message, validate_html_content, sanitize_field_name
from datetime import datetime
import functions_framework
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom
from bs4 import BeautifulSoup

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

def sanitize_filename(filename_base, identifier, use_identifier_only=False, logger_instance=None):
    """Sanitize filename for XML storage, prioritizing meaningful names."""
    if logger_instance is None:
        logger_instance = logging.getLogger(__name__)
    
    if use_identifier_only:
        sanitized_title = re.sub(r'[^\w\-_.]', '_', identifier).strip('._')[:150]
        return f"{sanitized_title}.xml"
    
    if not filename_base or not isinstance(filename_base, str) or filename_base == "Not Available":
        logger_instance.warning(f"No valid filename_base for {identifier}, trying fallbacks.")
        filename_base = f"document_{identifier}"
    
    sanitized_title = re.sub(r'[^\w\-_.]', '_', filename_base).replace(' ', '_').strip('._')[:150]
    final_filename = f"{sanitized_title}.xml" if sanitized_title else f"document_{identifier}.xml"
    logger_instance.debug(f"Sanitized filename for {identifier}: {final_filename}")
    return final_filename

def clean_html_content(html_content, logger_instance):
    """Clean and format HTML content to ensure proper structure and readability."""
    if not html_content:
        logger_instance.warning("Empty HTML content provided for cleaning.")
        return None
    
    try:
        # Parse HTML with BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Ensure correct HTML structure
        if not soup.html:
            new_html = soup.new_tag('html')
            soup.append(new_html)
            soup = new_html
        if not soup.head:
            soup.append(soup.new_tag('head'))
        if not soup.body:
            soup.append(soup.new_tag('body'))
        
        # Move content to body if misplaced
        for tag in soup.find_all(True):
            if tag.name not in ['html', 'head', 'body'] and tag.parent.name != 'body':
                soup.body.append(tag.extract())
        
        # Remove scripts, styles, and unnecessary attributes
        for element in soup(['script', 'style']):
            element.decompose()
        for tag in soup.find_all(True):
            tag.attrs = {k: v for k, v in tag.attrs.items() if k in ['class', 'id']}
        
        # Ensure proper formatting
        formatted_html = soup.prettify()
        # Validate encoding
        formatted_html.encode('utf-8')
        logger_instance.debug("Successfully cleaned and formatted HTML content.")
        return formatted_html
    except Exception as e:
        logger_instance.error(f"Failed to clean HTML content: {str(e)}", exc_info=True)
        return html_content  # Fallback to original content

def custom_pretty_xml(element, xml_structure_config):
    """Format XML with proper indentation and CDATA sections."""
    rough_string = tostring(element, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    pretty_xml_intermediate = reparsed.toprettyxml(indent=xml_structure_config.get('indent', '  '))
    
    # Remove empty lines and extra whitespace
    pretty_xml_intermediate = '\n'.join(line for line in pretty_xml_intermediate.splitlines() if line.strip())
    
    final_xml_string = pretty_xml_intermediate
    for field_config in xml_structure_config.get('fields', []):
        if field_config.get('cdata', False):
            tag = field_config['tag']
            # Ensure CDATA sections are properly formatted without extra whitespace
            final_xml_string = re.sub(
                rf'(<{tag}>)\s*(.*?)\s*(</{tag}>)',
                lambda m: f'{m.group(1)}<![CDATA[{m.group(2).strip()}]]>{m.group(3)}',
                final_xml_string,
                flags=re.DOTALL
            )
    
    if xml_structure_config.get('declaration', True):
        if not final_xml_string.strip().startswith('<?xml'):
            final_xml_string = '<?xml version="1.0" encoding="UTF-8"?>\n' + final_xml_string
        else:
            final_xml_string = re.sub(
                r'<\?xml.*?\?>',
                '<?xml version="1.0" encoding="UTF-8"?>',
                final_xml_string,
                count=1,
                flags=re.IGNORECASE
            )
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
        required_fields = project_config.get('required_fields', [])
        search_required_fields = project_config.get('search_required_fields', [])

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

        # Check if XML already exists
        if doc_data.get('xml_path') and doc_data.get('xml_status') == 'Success':
            active_logger.info(f"XML already generated for {identifier} at {doc_data['xml_path']}. Skipping regeneration.")
            return {'status': 'skipped_existing', 'identifier': identifier, 'xml_path': doc_data['xml_path']}

        html_content = validate_html_content(doc_data.get('fixed_html_content_in_fs'), active_logger)
        html_content = clean_html_content(html_content, active_logger) if html_content else None

        # Fallback to GCS if fixed_html_content_in_fs is missing or invalid
        if not html_content:
            active_logger.warning(f"No valid fixed_html_content_in_fs for {identifier}. Falling back to GCS html_path.")
            html_path_final_gcs = doc_data.get('html_path')
            if not html_path_final_gcs or not html_path_final_gcs.startswith(f"gs://{bucket_name}/"):
                error_msg = f"Invalid or missing GCS html_path for {identifier}: {html_path_final_gcs}"
                active_logger.error(error_msg)
                doc_ref.update({
                    "xml_status": "Error: Missing valid GCS HTML path",
                    "last_updated": firestore.SERVER_TIMESTAMP
                })
                raise ValueError(error_msg)

            try:
                blob_name_final = html_path_final_gcs.replace(f"gs://{bucket_name}/", "")
                blob_final = bucket.blob(blob_name_final)
                if not blob_final.exists():
                    raise FileNotFoundError(f"HTML blob {blob_name_final} not found in GCS.")
                if blob_name_final.endswith('.gz'):
                    gzipped_content_final = blob_final.download_as_bytes()
                    html_content = validate_html_content(gzip.decompress(gzipped_content_final), active_logger)
                else:
                    html_content = validate_html_content(blob_final.download_as_text(), active_logger)
                html_content = clean_html_content(html_content, active_logger)
                if not html_content:
                    raise ValueError("Invalid HTML content from GCS.")
                active_logger.info(f"Successfully downloaded and cleaned HTML for {identifier} from {html_path_final_gcs}")
            except Exception as e_gcs_download:
                active_logger.error(f"Failed to download HTML from GCS for {identifier} ({html_path_final_gcs}): {e_gcs_download}", exc_info=True)
                doc_ref.update({
                    "xml_status": f"Error: Failed GCS HTML download {str(e_gcs_download)[:200]}",
                    "last_updated": firestore.SERVER_TIMESTAMP
                })
                raise

        active_logger.info(f"Using HTML content for {identifier} (source: {'fixed_html_content_in_fs' if doc_data.get('fixed_html_content_in_fs') else 'GCS'})")

        # Validate required fields before XML generation
        missing_critical_fields = []
        for field in required_fields:
            sanitized_field = sanitize_field_name(field)
            field_value = get_mapped_field(doc_data, sanitized_field, field_mappings, logger_instance=active_logger)
            if field_value == "Not Available" or field_value is None:
                missing_critical_fields.append(sanitized_field)
                active_logger.warning(f"Critical field '{sanitized_field}' missing for {identifier}")

        if missing_critical_fields:
            error_msg = f"Cannot generate XML for {identifier}: Missing critical fields {missing_critical_fields}"
            active_logger.error(error_msg)
            doc_ref.update({
                "xml_status": f"Error: Missing critical fields {', '.join(missing_critical_fields)}",
                "last_updated": firestore.SERVER_TIMESTAMP
            })
            raise ValueError(error_msg)

        try:
            root = Element(xml_structure_config.get('root_tag', 'GermanyFederalLaw'))
            missing_fields_list = []
            for field_config in xml_structure_config.get('fields', []):
                tag_name = field_config['tag']
                source_field_name = field_config['source']
                sanitized_field = sanitize_field_name(source_field_name)
                field_value_str = "Not Available"

                if source_field_name == "fixed_html_content":
                    field_value_str = html_content if html_content else "Not Available"
                    if field_value_str == "Not Available":
                        missing_fields_list.append("fixed_html_content")
                        active_logger.warning(f"HTML content missing for {identifier}")
                else:
                    field_value = get_mapped_field(doc_data, sanitized_field, field_mappings, logger_instance=active_logger)
                    field_value_str = str(field_value) if field_value is not None else str(doc_data.get(sanitized_field, "Not Available"))
                    if field_value_str == "Not Available":
                        missing_fields_list.append(sanitized_field)
                        active_logger.warning(f"Field '{sanitized_field}' missing or Not Available for {identifier}")

                sub_element = SubElement(root, tag_name)
                sub_element.text = field_value_str

            if missing_fields_list:
                active_logger.info(f"Generated XML for {identifier} with non-critical missing fields: {missing_fields_list}")

            xml_bytes = custom_pretty_xml(root, xml_structure_config)
        except Exception as e_xml_build:
            active_logger.error(f"Failed to build XML for {identifier}: {e_xml_build}", exc_info=True)
            for attempt in range(MAX_FIRESTORE_RETRIES):
                try:
                    doc_ref.update({
                        "xml_status": f"Error: XML build failed {str(e_xml_build)[:200]}",
                        "last_updated": firestore.SERVER_TIMESTAMP
                    })
                    break
                except Exception as e_update:
                    active_logger.warning(f"Firestore update failed on attempt {attempt + 1}: {str(e_update)}")
                    if attempt < MAX_FIRESTORE_RETRIES - 1:
                        time.sleep(RETRY_BACKOFF * (2 ** attempt))
                    else:
                        active_logger.error(f"Max retries reached for Firestore update: {str(e_update)}")
                        raise
            raise

        filename_field = xml_structure_config.get('filename_field', 'fullname')
        filename_value = get_mapped_field(doc_data, sanitize_field_name(filename_field), field_mappings, logger_instance=active_logger)
        if filename_value and filename_value != "Not Available":
            xml_filename_base = filename_value
        else:
            law_id = doc_data.get(sanitize_field_name("Law-ID"), "Unknown_LawID")
            abbreviation = doc_data.get(sanitize_field_name("Abbreviation"), "Unknown_Abbreviation")
            if law_id != "Unknown_LawID" and abbreviation != "Unknown_Abbreviation":
                xml_filename_base = f"{law_id}_{abbreviation}"
                active_logger.info(f"Using Law-ID and Abbreviation for filename: {xml_filename_base}")
            else:
                xml_filename_base = f"document_{identifier}"
                active_logger.warning(f"Falling back to default filename for {identifier}: {xml_filename_base}")
        
        xml_filename_sanitized = sanitize_filename(xml_filename_base, identifier, use_identifier_only=False, logger_instance=active_logger)

        xml_gcs_output_path_template = xml_structure_config.get('gcs_xml_path', f"delivered_xml/{project_id}/<date>")
        final_xml_gcs_path_template = xml_gcs_output_path_template.replace(
            "<project_id>", "".join(c if c.isalnum() else '_' for c in project_id)
        ).replace("<date>", date)
        xml_destination_gcs_path = f"{final_xml_gcs_path_template.rstrip('/')}/{xml_filename_sanitized}"

        xml_blob_final = bucket.blob(xml_destination_gcs_path)
        try:
            for attempt in range(MAX_GCS_RETRIES):
                try:
                    xml_blob_final.upload_from_string(xml_bytes, content_type='application/xml; charset=utf-8')
                    active_logger.info(f"Saved XML for {identifier} to gs://{bucket_name}/{xml_destination_gcs_path}")
                    break
                except Exception as e_upload:
                    active_logger.warning(f"GCS upload failed for {xml_destination_gcs_path} on attempt {attempt + 1}: {str(e_upload)}")
                    if attempt < MAX_GCS_RETRIES - 1:
                        time.sleep(RETRY_BACKOFF * (2 ** attempt))
                    else:
                        raise Exception(f"Max retries reached for GCS upload: {str(e_upload)}")
        except Exception as e_xml_upload:
            active_logger.error(f"Failed to upload XML for {identifier}: {e_xml_upload}", exc_info=True)
            for attempt in range(MAX_FIRESTORE_RETRIES):
                try:
                    doc_ref.update({
                        "xml_status": f"Error: XML GCS upload failed {str(e_xml_upload)[:200]}",
                        "last_updated": firestore.SERVER_TIMESTAMP
                    })
                    break
                except Exception as e_update:
                    active_logger.warning(f"Firestore update failed on attempt {attempt + 1}: {str(e_update)}")
                    if attempt < MAX_FIRESTORE_RETRIES - 1:
                        time.sleep(RETRY_BACKOFF * (2 ** attempt))
                    else:
                        active_logger.error(f"Max retries reached for Firestore update: {str(e_update)}")
                        raise
            raise

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
                active_logger.info(f"Updated Firestore for {identifier} with XML path and status.")
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

        return {
            'status': 'success',
            'identifier': identifier,
            'xml_path': f"gs://{bucket_name}/{xml_destination_gcs_path}"
        }

    except Exception as e_critical:
        active_logger.error(f"Critical error in generate_xml for identifier '{identifier}': {str(e_critical)}", exc_info=True)
        if 'data' in locals() and data.get('customer') and data.get('project'):
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
                active_logger.info("Published critical error to retry topic.")
            else:
                active_logger.error("GCP Project ID for error reporting not found.")
        raise