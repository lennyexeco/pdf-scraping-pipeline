import json
import base64
import logging
import re
import os
import time
import gzip
from google.cloud import firestore, pubsub_v1, storage
from src.common.utils import setup_logging
from src.common.config import load_customer_config, load_dynamic_site_config
from src.common.helpers import get_mapped_field, sanitize_error_message, validate_html_content
from datetime import datetime
import functions_framework
from xml.etree.ElementTree import Element, SubElement, tostring, ProcessingInstruction
from xml.dom import minidom

logger = logging.getLogger(__name__)

RETRY_TOPIC_NAME = "retry-pipeline"
NEXT_STEP_GENERATE_REPORTS_TOPIC_NAME = "generate-reports-topic"
MAX_GCS_RETRIES = 3
MAX_FIRESTORE_RETRIES = 3
RETRY_BACKOFF = 5

def serialize_firestore_doc(data):
    if isinstance(data, dict):
        return {k: serialize_firestore_doc(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [serialize_firestore_doc(item) for item in data]
    elif isinstance(data, datetime):
        return data.isoformat()
    return data

def sanitize_xml_filename_from_base(indexed_filename_base, xml_config, logger_instance):
    """
    Generate XML filename using the indexed_filename_base and template.
    """
    template = xml_config.get('filename_template', '{indexed_filename_base}.xml')
    
    try:
        filename_base_from_template = template.format(indexed_filename_base=indexed_filename_base)
    except KeyError as e:
        logger_instance.warning(f"Missing key {e} for filename template '{template}'. Using '{indexed_filename_base}.xml' as base.")
        filename_base_from_template = f"{indexed_filename_base}.xml"

    if not filename_base_from_template.lower().endswith('.xml'):
        filename_base_from_template += ".xml"
    
    # Basic sanitization for the generated name (should be minimal if base is clean)
    filename_base_sanitized = re.sub(r'[/\\]', '_', filename_base_from_template)
    name_part, ext_part = os.path.splitext(filename_base_sanitized)
    
    # Ensure name_part is not empty after sanitization
    sanitized_name_part = re.sub(r'[^\w\-_.]', '', name_part).replace(' ', '_').strip('._')
    final_filename = (sanitized_name_part[:200] if sanitized_name_part else indexed_filename_base) + (ext_part if ext_part else ".xml")
    
    logger_instance.debug(f"Sanitized XML filename using base '{indexed_filename_base}': {final_filename}")
    return final_filename


def clean_html_for_xml(html_content, logger_instance):
    if not html_content:
        logger_instance.warning("Empty HTML content provided for XML cleaning.")
        return ""
    try:
        validated_html = validate_html_content(html_content, logger_instance)
        if not validated_html:
            return ""
        if not isinstance(validated_html, str):
            validated_html = str(validated_html)
        cleaned_html = validated_html.replace('\x00', '')
        logger_instance.debug("HTML content prepared for CDATA embedding.")
        return cleaned_html
    except Exception as e:
        logger_instance.error(f"Failed to clean HTML content for XML: {str(e)}", exc_info=True)
        return str(html_content).replace('\x00', '')


def custom_pretty_xml(element, xml_structure_config):
    rough_string = tostring(element, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    
    indent_str = xml_structure_config.get('indent', '  ')
    pretty_xml_intermediate = reparsed.toprettyxml(indent=indent_str)
    pretty_xml_intermediate = os.linesep.join([s for s in pretty_xml_intermediate.splitlines() if s.strip()])
    final_xml_string = pretty_xml_intermediate

    # Ensure field_conf.get('target_tag') is also checked as some configs might use that
    for field_conf in xml_structure_config.get('fields', []):
        if field_conf.get('cdata', False):
            tag = field_conf.get('tag') or field_conf.get('target_tag') 
            if not tag:
                continue
            pattern = re.compile(rf'(<{tag}[^>]*>)(.*?)(</{tag}>)', re.DOTALL)
            
            def cdata_replacer(match):
                content = match.group(2).strip()
                if content.startswith("<![CDATA[") and content.endswith("]]>"):
                    return match.group(0) 
                return f"{match.group(1)}<![CDATA[{content}]]>{match.group(3)}"
            final_xml_string = pattern.sub(cdata_replacer, final_xml_string)

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
        final_xml_string = re.sub(r'<\?xml.*?\?>\s*\n?', '', final_xml_string, count=1, flags=re.IGNORECASE).lstrip()
    
    return final_xml_string.encode('utf-8')


def generate_structured_data_content(doc_data, logger_instance):
    """
    Generate content for structured data documents (table entries).
    """
    table_row_data = doc_data.get('table_row_data', [])
    table_context = doc_data.get('table_context', '')
    description = doc_data.get('Summary_Description', '')
    
    if not table_row_data:
        logger_instance.warning("No table row data found for structured data document")
        return description or "Structured data entry"
    
    # Create a formatted representation of the table data
    content_lines = []
    if table_context:
        content_lines.append(f"Table Context: {table_context}")
    
    if description:
        content_lines.append(f"Description: {description}")
    
    content_lines.append("Table Data:")
    for i, cell_data in enumerate(table_row_data):
        if cell_data.strip():
            content_lines.append(f"  Column {i + 1}: {cell_data}")
    
    return "\n".join(content_lines)


def get_content_for_document_type(doc_data, html_content_str, logger_instance):
    """
    Get appropriate content based on document type.
    """
    document_type = doc_data.get('document_type', 'HTML_DOCUMENT')
    
    if document_type == 'STRUCTURED_DATA':
        # For structured data, generate content from table data
        return generate_structured_data_content(doc_data, logger_instance)
    elif document_type == 'PDF_DOCUMENT':
        # For PDF documents, create a simple content description
        title = doc_data.get('Document_Title', 'PDF Document')
        description = doc_data.get('Summary_Description', '')
        pdf_paths = doc_data.get('pdf_gcs_paths', [])
        
        content_lines = [f"PDF Document: {title}"]
        if description:
            content_lines.append(f"Description: {description}")
        if pdf_paths:
            content_lines.append("PDF Files:")
            for pdf_path in pdf_paths:
                content_lines.append(f"  - {pdf_path}")
        
        return "\n".join(content_lines)
    else:
        # HTML_DOCUMENT - return the actual HTML content
        return html_content_str


@functions_framework.cloud_event
def generate_xml(cloud_event):
    active_logger = logger
    pubsub_message = {}
    gcp_project_id_for_error = os.environ.get("GCP_PROJECT")
    publisher_for_error = None
    identifier = "unknown_identifier"
    doc_data = {}

    try:
        event_data = cloud_event.data
        if 'message' in event_data and 'data' in event_data['message']:
            pubsub_message_data = event_data['message']['data']
        else:
            active_logger.error("No 'message' or 'data' in Pub/Sub event.")
            raise ValueError("Invalid Pub/Sub message structure.")

        pubsub_message = json.loads(base64.b64decode(pubsub_message_data).decode('utf-8'))
        
        customer_id = pubsub_message.get('customer')
        project_id_config_name = pubsub_message.get('project')
        identifier = pubsub_message.get('identifier')
        date_str = pubsub_message.get('date', datetime.now().strftime('%Y%m%d'))

        missing_fields = [f for f, v in {"customer": customer_id, "project": project_id_config_name, "identifier": identifier}.items() if not v]
        if missing_fields:
            error_msg = f"Missing required Pub/Sub fields: {', '.join(missing_fields)}"
            active_logger.error(error_msg)
            raise ValueError(error_msg)

        active_logger = setup_logging(customer_id, project_id_config_name)
        active_logger.info(f"Starting generate_xml for identifier: {identifier}, project: {project_id_config_name}, date: {date_str}")

        customer_config = load_customer_config(customer_id)
        gcp_project_id = customer_config.get("gcp_project_id", os.environ.get("GCP_PROJECT"))
        gcp_project_id_for_error = gcp_project_id
        
        if not gcp_project_id:
            active_logger.error("GCP Project ID not configured.")
            raise ValueError("GCP Project ID not configured")

        firestore_db_id_for_config = customer_config.get('firestore_database_id_for_config', '(default)')
        db_options_config = {"project": gcp_project_id}
        if firestore_db_id_for_config != "(default)":
            db_options_config["database"] = firestore_db_id_for_config
        db_for_config = firestore.Client(**db_options_config)
        
        project_config = load_dynamic_site_config(db_for_config, project_id_config_name, active_logger)

        gcs_bucket_name = project_config.get('gcs_bucket')
        firestore_collection_name = project_config.get('firestore_collection')
        firestore_db_id_operational = project_config.get('firestore_database_id', '(default)') 
        
        xml_structure_config = project_config.get('xml_structure')
        field_mappings = project_config.get('field_mappings', {})

        if not all([gcs_bucket_name, firestore_collection_name, xml_structure_config]):
            error_msg = "Missing critical configuration: gcs_bucket, firestore_collection, or xml_structure."
            active_logger.error(error_msg)
            raise ValueError(error_msg)

        storage_client = storage.Client(project=gcp_project_id)
        gcs_bucket = storage_client.bucket(gcs_bucket_name)
        
        db_options_operational = {"project": gcp_project_id}
        if firestore_db_id_operational != "(default)":
            db_options_operational["database"] = firestore_db_id_operational
        db_operational = firestore.Client(**db_options_operational)
        
        publisher_for_error = pubsub_v1.PublisherClient()

        doc_ref = db_operational.collection(firestore_collection_name).document(identifier)
        doc = None
        for attempt in range(MAX_FIRESTORE_RETRIES):
            try:
                doc = doc_ref.get()
                if doc.exists:
                    break
                active_logger.warning(f"Firestore doc {identifier} not found (attempt {attempt + 1}). Retrying...")
                time.sleep(RETRY_BACKOFF * (2 ** attempt))
            except Exception as e_fs_fetch:
                active_logger.warning(f"Firestore fetch for {identifier} failed (attempt {attempt + 1}): {str(e_fs_fetch)}")
                if attempt < MAX_FIRESTORE_RETRIES - 1:
                    time.sleep(RETRY_BACKOFF * (2 ** attempt))
                else:
                    active_logger.error(f"Max retries for Firestore fetch ({identifier}): {str(e_fs_fetch)}")
                    raise ValueError(f"Failed to fetch Firestore document {identifier}: {str(e_fs_fetch)}")
        
        if not doc or not doc.exists:
            active_logger.error(f"Firestore document {identifier} not found in {firestore_collection_name}.")
            return {'status': 'error', 'message': f'Firestore document {identifier} not found.'}

        doc_data = doc.to_dict()
        indexed_filename_base = doc_data.get("indexed_filename_base")
        document_type = doc_data.get("document_type", "HTML_DOCUMENT")  # NEW: Get document type

        active_logger.info(f"Processing {document_type} for XML generation: {identifier}")

        if not indexed_filename_base:
            error_msg = f"indexed_filename_base not found in Firestore for {identifier}."
            active_logger.error(error_msg)
            doc_ref.update({"xml_status": f"Error: {error_msg}", "processing_stage": "xml_error_missing_idx_filename_base", "last_updated": firestore.SERVER_TIMESTAMP})
            raise ValueError(error_msg)

        if doc_data.get('xml_path') and doc_data.get('xml_status') == 'Success' and doc_data.get('processing_stage') == 'xml_generated':
            active_logger.info(f"XML already generated for {identifier} at {doc_data['xml_path']}. Skipping.")
            return {'status': 'skipped_existing', 'identifier': identifier, 'xml_path': doc_data['xml_path']}

        # NEW: Handle content based on document type
        html_content_str = ""
        content_for_xml = ""
        
        if document_type == "HTML_DOCUMENT":
            # Existing logic for HTML documents
            html_content_gcs_path = doc_data.get('html_gcs_path')
            if not html_content_gcs_path or not html_content_gcs_path.startswith(f"gs://{gcs_bucket_name}/"):
                error_msg = f"Invalid or missing GCS HTML path for HTML document {identifier}: {html_content_gcs_path}"
                active_logger.error(error_msg)
                doc_ref.update({"xml_status": f"Error: {error_msg}", "processing_stage": "xml_error_html_path_missing", "last_updated": firestore.SERVER_TIMESTAMP})
                raise ValueError(error_msg)

            try:
                blob_name_from_path = html_content_gcs_path.replace(f"gs://{gcs_bucket_name}/", "")
                html_blob = gcs_bucket.blob(blob_name_from_path)
                
                if not html_blob.exists():
                    raise FileNotFoundError(f"HTML blob {blob_name_from_path} not found in GCS for {identifier}.")

                if blob_name_from_path.endswith('.gz'):
                    gzipped_content = html_blob.download_as_bytes()
                    html_content_str = gzip.decompress(gzipped_content).decode('utf-8')
                else:
                    html_content_str = html_blob.download_as_text(encoding='utf-8')
                
                html_content_str = clean_html_for_xml(html_content_str, active_logger)
                if not html_content_str and xml_structure_config.get('require_html_content', True):
                     raise ValueError("Cleaned HTML content is empty, but XML generation requires HTML.")
                active_logger.info(f"Successfully downloaded HTML for {identifier} from {html_content_gcs_path}")

            except Exception as e_gcs_html:
                error_msg = f"Failed to get/process HTML from GCS for {identifier} ({html_content_gcs_path}): {str(e_gcs_html)}"
                active_logger.error(error_msg, exc_info=True)
                doc_ref.update({"xml_status": f"Error: {sanitize_error_message(error_msg)}", "processing_stage": "xml_error_gcs_html_download", "last_updated": firestore.SERVER_TIMESTAMP})
                raise
                
        elif document_type in ["PDF_DOCUMENT", "STRUCTURED_DATA"]:
            # For PDF and structured data, no HTML content to fetch
            active_logger.info(f"Skipping HTML fetch for {document_type}: {identifier}")
            html_content_str = ""  # No HTML content for these types
        
        # NEW: Get appropriate content based on document type
        content_for_xml = get_content_for_document_type(doc_data, html_content_str, active_logger)

        # Build XML structure
        xml_root_tag = xml_structure_config.get('root_tag', 'document')
        root_element = Element(xml_root_tag)
        
        # NEW: Add document type as root attribute for better categorization
        root_element.set('document_type', document_type)
        
        # NEW: Add source tracking attributes
        source_category_url = doc_data.get('source_category_url')
        source_page_url = doc_data.get('source_page_url')
        if source_category_url:
            root_element.set('source_category_url', source_category_url)
        if source_page_url:
            root_element.set('source_page_url', source_page_url)
        
        processing_instructions = xml_structure_config.get('processing_instructions', [])
        for pi_target, pi_text in processing_instructions:
            root_element.append(ProcessingInstruction(pi_target, pi_text))

        missing_fields_log = []
        # Ensure doc_data contains the indexed_filename_base for mapping if needed
        doc_data_for_mapping = {**doc_data, "indexed_filename_base": indexed_filename_base}

        for field_conf in xml_structure_config.get('fields', []):
            tag_name = field_conf.get('tag') or field_conf.get('target_tag')
            source_firestore_field = field_conf.get('source')
            
            if not tag_name or not source_firestore_field:
                active_logger.warning(f"Skipping field config due to missing 'tag' or 'source': {field_conf}")
                continue

            field_value_str = ""
            
            # NEW: Handle different content sources based on document type
            if source_firestore_field == "gcs_html_content":
                field_value_str = content_for_xml  # Use the type-specific content
            elif source_firestore_field == "document_content":  # NEW: Generic content field
                field_value_str = content_for_xml
            else:
                # Use doc_data_for_mapping which includes indexed_filename_base
                raw_value = get_mapped_field(doc_data_for_mapping, source_firestore_field, field_mappings, logger_instance=active_logger)
                
                if raw_value is None or raw_value == "Not Available":
                    field_value_str = field_conf.get('default_value', '')
                    if field_conf.get('required', False) and not field_value_str:
                        error_msg = f"Required XML field '{tag_name}' (source: '{source_firestore_field}') missing for {identifier}."
                        active_logger.error(error_msg)
                        doc_ref.update({"xml_status": f"Error: {sanitize_error_message(error_msg)}", "processing_stage": "xml_error_missing_required_field", "last_updated": firestore.SERVER_TIMESTAMP})
                        raise ValueError(error_msg)
                    missing_fields_log.append(f"{tag_name} (source: {source_firestore_field})")
                elif isinstance(raw_value, bool):
                    field_value_str = str(raw_value).lower()
                elif isinstance(raw_value, datetime):
                    field_value_str = raw_value.isoformat()
                elif isinstance(raw_value, list):
                    # NEW: Better list handling for different types
                    if all(isinstance(item, str) for item in raw_value):
                        field_value_str = ", ".join(raw_value)
                    else:
                        field_value_str = ", ".join(map(str, raw_value))
                elif isinstance(raw_value, firestore.SERVER_TIMESTAMP.__class__): 
                    field_value_str = datetime.now().isoformat() 
                else:
                    field_value_str = str(raw_value)

            sub_element = SubElement(root_element, tag_name)
            sub_element.text = field_value_str

            # Handle attributes
            for attr_name, attr_source_field in field_conf.get('attributes', {}).items():
                attr_value = get_mapped_field(doc_data_for_mapping, attr_source_field, field_mappings, logger_instance=active_logger)
                if attr_value is not None and attr_value != "Not Available":
                    sub_element.set(attr_name, str(attr_value))
                else:
                    active_logger.debug(f"Attribute '{attr_name}' for tag '{tag_name}' (source: '{attr_source_field}') not found for {identifier}.")

        if missing_fields_log:
            active_logger.info(f"XML for {identifier} generated with some missing/defaulted fields: {', '.join(missing_fields_log)}")

        try:
            xml_bytes_output = custom_pretty_xml(root_element, xml_structure_config)
        except Exception as e_xml_format:
            error_msg = f"Failed to format XML for {identifier}: {str(e_xml_format)}"
            active_logger.error(error_msg, exc_info=True)
            doc_ref.update({"xml_status": f"Error: {sanitize_error_message(error_msg)}", "processing_stage": "xml_error_formatting", "last_updated": firestore.SERVER_TIMESTAMP})
            raise
        
        # Use the existing sanitize_xml_filename_from_base function
        xml_final_filename = sanitize_xml_filename_from_base(
            indexed_filename_base=indexed_filename_base,
            xml_config=xml_structure_config, 
            logger_instance=active_logger
        )

        project_id_sanitized = "".join(c if c.isalnum() else '_' for c in project_id_config_name)
        gcs_xml_path_template = xml_structure_config.get('gcs_xml_output_path', f"xml_output/{project_id_sanitized}/{{date_str}}")
        
        final_xml_gcs_folder = gcs_xml_path_template.format(project_id_sanitized=project_id_sanitized, date_str=date_str)
        xml_destination_gcs_full_path = f"{final_xml_gcs_folder.rstrip('/')}/{xml_final_filename}"

        xml_blob_gcs = gcs_bucket.blob(xml_destination_gcs_full_path)
        try:
            for attempt in range(MAX_GCS_RETRIES):
                try:
                    xml_blob_gcs.upload_from_string(xml_bytes_output, content_type='application/xml; charset=utf-8')
                    active_logger.info(f"Saved {document_type} XML for {identifier} to gs://{gcs_bucket_name}/{xml_destination_gcs_full_path}")
                    break
                except Exception as e_gcs_upload:
                    active_logger.warning(f"GCS XML upload for {xml_destination_gcs_full_path} failed (attempt {attempt + 1}): {str(e_gcs_upload)}")
                    if attempt < MAX_GCS_RETRIES - 1:
                        time.sleep(RETRY_BACKOFF * (2 ** attempt))
                    else:
                        raise Exception(f"Max retries for GCS XML upload: {str(e_gcs_upload)}")
        except Exception as e_final_xml_upload:
            error_msg = f"Failed to upload final XML for {identifier} to GCS: {str(e_final_xml_upload)}"
            active_logger.error(error_msg, exc_info=True)
            doc_ref.update({"xml_status": f"Error: {sanitize_error_message(error_msg)}", "processing_stage": "xml_error_gcs_upload", "last_updated": firestore.SERVER_TIMESTAMP})
            raise

        final_xml_gcs_uri = f"gs://{gcs_bucket_name}/{xml_destination_gcs_full_path}"
        try:
            for attempt in range(MAX_FIRESTORE_RETRIES):
                try:
                    doc_ref.update({
                        'xml_path': final_xml_gcs_uri,
                        'xml_filename': xml_final_filename,
                        'xml_status': 'Success',
                        'xml_generated_timestamp': firestore.SERVER_TIMESTAMP,
                        'processing_stage': 'xml_generated',
                        'last_updated': firestore.SERVER_TIMESTAMP,
                        'last_error': firestore.DELETE_FIELD 
                    })
                    active_logger.info(f"Successfully updated Firestore for {identifier} with XML generation status.")
                    break
                except Exception as e_fs_update_final:
                    active_logger.warning(f"Firestore final update for {identifier} failed (attempt {attempt+1}): {str(e_fs_update_final)}")
                    if attempt < MAX_FIRESTORE_RETRIES - 1:
                        time.sleep(RETRY_BACKOFF * (2 ** attempt))
                    else:
                        active_logger.error(f"Max retries for Firestore final update ({identifier}): {str(e_fs_update_final)}. XML is at {final_xml_gcs_uri}")
                        raise
        except Exception as e_fs_final_update_outer:
             active_logger.error(f"Outer error during Firestore final update for {identifier}: {str(e_fs_final_update_outer)}", exc_info=True)
             raise

        # Trigger report generation if enabled
        if project_config.get("trigger_generate_reports_after_xml", True):
            report_payload = {
                'customer': customer_id,
                'project': project_id_config_name,
                'identifier': identifier,
                'status': 'xml_generation_completed',
                'date': date_str,
                'xml_output_path': final_xml_gcs_uri,
                'document_type': document_type  # NEW: Include document type in report payload
            }
            publisher_for_error.publish(
                publisher_for_error.topic_path(gcp_project_id, NEXT_STEP_GENERATE_REPORTS_TOPIC_NAME),
                json.dumps(report_payload).encode('utf-8')
            )
            active_logger.info(f"Published to '{NEXT_STEP_GENERATE_REPORTS_TOPIC_NAME}' for {identifier} after XML generation.")

        return {'status': 'success', 'identifier': identifier, 'xml_path': final_xml_gcs_uri, 'document_type': document_type}

    except Exception as e_critical:
        active_logger.error(f"Critical error in generate_xml for identifier '{identifier}': {str(e_critical)}", exc_info=True)
        if gcp_project_id_for_error and publisher_for_error:
            retry_payload = {
                "customer": pubsub_message.get("customer", "unknown_customer"),
                "project": pubsub_message.get("project", "unknown_project"),
                "original_pubsub_message": pubsub_message,
                "error_message": sanitize_error_message(str(e_critical)),
                "stage": "generate_xml_critical",
                "retry_count": pubsub_message.get("retry_count", 0) + 1,
                "identifier_in_error": identifier,
                "doc_data_snapshot": serialize_firestore_doc(doc_data) if doc_data else None
            }
            try:
                publisher_for_error.publish(
                    publisher_for_error.topic_path(gcp_project_id_for_error, RETRY_TOPIC_NAME),
                    json.dumps(retry_payload, default=str).encode('utf-8')
                )
                active_logger.info(f"Published critical error for {identifier} from generate_xml to topic '{RETRY_TOPIC_NAME}'.")
            except Exception as e_retry_pub:
                active_logger.error(f"Failed to publish critical error for {identifier} from generate_xml to retry topic: {str(e_retry_pub)}")
        else:
            active_logger.error("Cannot publish to retry topic: GCP Project ID or Publisher client not initialized.")
        raise