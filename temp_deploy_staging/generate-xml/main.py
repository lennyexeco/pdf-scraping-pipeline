# src/functions/generate_xml/main.py

import json
import base64
import logging
import re
import os
import time
import gzip
from google.cloud import firestore, pubsub_v1, storage
from src.common.utils import setup_logging
from src.common.config import load_customer_config, load_dynamic_site_config # Use dynamic
from src.common.helpers import get_mapped_field, sanitize_error_message, validate_html_content
from datetime import datetime, timezone # Added timezone
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
    """Helper to serialize Firestore data, especially for error snapshots."""
    if isinstance(data, dict):
        return {k: serialize_firestore_doc(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [serialize_firestore_doc(item) for item in data]
    elif isinstance(data, datetime):
        return data.isoformat()
    # Add other type handlers if necessary (e.g., firestore.SERVER_TIMESTAMP)
    return data

def sanitize_xml_filename_from_base(indexed_filename_base, xml_config, logger_instance):
    """
    Generates and sanitizes an XML filename using the indexed_filename_base and a template.
    """
    template = xml_config.get('filename_template', '{indexed_filename_base}.xml')
    try:
        filename_base_from_template = template.format(indexed_filename_base=indexed_filename_base)
    except KeyError as e:
        logger_instance.warning(f"Missing key {e} for filename template '{template}'. Using '{indexed_filename_base}.xml' as base.")
        filename_base_from_template = f"{indexed_filename_base}.xml"

    if not filename_base_from_template.lower().endswith('.xml'):
        filename_base_from_template += ".xml"
    
    # Basic sanitization for the generated name
    filename_base_sanitized = re.sub(r'[/\\]', '_', filename_base_from_template) # Replace slashes
    name_part, ext_part = os.path.splitext(filename_base_sanitized)
    
    # Further sanitize name_part: remove invalid characters, replace spaces, strip leading/trailing ., _
    sanitized_name_part = re.sub(r'[^\w\-_.]', '', name_part).replace(' ', '_').strip('._')
    # Ensure name_part is not empty and not excessively long (e.g., GCS object name limits)
    final_filename = (sanitized_name_part[:200] if sanitized_name_part else indexed_filename_base[:200]) + (ext_part if ext_part else ".xml")
    
    logger_instance.debug(f"Sanitized XML filename using base '{indexed_filename_base}': {final_filename}")
    return final_filename


def clean_html_for_xml(html_content_bytes, logger_instance):
    """
    Cleans HTML content (provided as bytes) for embedding within XML CDATA.
    Decodes bytes, validates, and removes characters invalid in XML.
    """
    if not html_content_bytes:
        logger_instance.warning("Empty HTML content bytes provided for XML cleaning.")
        return ""
    try:
        # Try decoding with utf-8 first, then fall back if needed
        try:
            html_content_str = html_content_bytes.decode('utf-8')
        except UnicodeDecodeError:
            logger_instance.warning("UTF-8 decoding failed for HTML content, trying latin-1 as fallback.")
            html_content_str = html_content_bytes.decode('latin-1', errors='replace') # Replace errors

        validated_html = validate_html_content(html_content_str, logger_instance) # validate_html_content expects string
        if not validated_html: # validate_html_content might return None or empty if invalid
            logger_instance.warning("HTML content validation returned empty or None.")
            return ""
        
        # Ensure it's a string after validation (should be, but defensive)
        if not isinstance(validated_html, str):
            validated_html = str(validated_html)
        
        # Remove null characters which are invalid in XML
        cleaned_html = validated_html.replace('\x00', '')
        # Remove other control characters typically problematic for XML, excluding tab, LF, CR
        cleaned_html = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', cleaned_html)

        logger_instance.debug("HTML content successfully cleaned for CDATA embedding.")
        return cleaned_html
    except Exception as e:
        logger_instance.error(f"Failed to clean HTML content for XML: {str(e)}", exc_info=True)
        # Fallback: attempt a simple decode and null character removal
        try:
            return html_content_bytes.decode('utf-8', errors='replace').replace('\x00', '')
        except Exception as fallback_e:
            logger_instance.error(f"Final fallback decoding/cleaning failed: {fallback_e}")
            # As a last resort, return a string representation, though it might be problematic
            return str(html_content_bytes, errors='ignore').replace('\x00', '')


def custom_pretty_xml(element, xml_structure_config):
    """
    Generates a pretty-printed XML string from an ElementTree element,
    handling CDATA sections and XML declaration based on config.
    """
    rough_string = tostring(element, 'utf-8') # ElementTree tostring
    reparsed = minidom.parseString(rough_string) # Parse with minidom for pretty printing
    
    indent_str = xml_structure_config.get('indent', '  ')
    # Use toprettyxml for initial formatting, then clean up extra newlines from minidom
    pretty_xml_intermediate = reparsed.toprettyxml(indent=indent_str, encoding='utf-8').decode('utf-8')
    
    # Remove empty lines that minidom might add
    pretty_xml_lines = [s for s in pretty_xml_intermediate.splitlines() if s.strip()]
    final_xml_string = os.linesep.join(pretty_xml_lines)

    # Re-wrap content of tags marked for CDATA
    # This needs to be done carefully AFTER initial pretty printing
    # because minidom will escape characters inside tags, which we don't want for CDATA.
    # The strategy here is to identify tags, extract their already-escaped content,
    # unescape it (if minidom did that), and then wrap with CDATA.
    # However, tostring() from ElementTree with proper CDATA sections (if built with them)
    # might be more direct. The current approach with minidom assumes text content was set
    # and minidom handles the text node. If CDATA is critical, building elements
    # with explicit CDATA type might be better before minidom.
    # For now, this tries to retrofit CDATA:
    
    temp_final_xml_string = final_xml_string # Work on a copy for CDATA replacement
    for field_conf in xml_structure_config.get('fields', []):
        if field_conf.get('cdata', False):
            tag = field_conf.get('tag') or field_conf.get('target_tag') 
            if not tag:
                continue
            
            # Regex to find content within the specified tag. Non-greedy match for content.
            # This pattern looks for <tag>content</tag> or <tag attr="val">content</tag>
            pattern = re.compile(rf"(<{tag}(?:\s+[^>]*)?>)(.*?)(</{tag}>)", re.DOTALL)
            
            def cdata_replacer(match):
                opening_tag = match.group(1)
                content = match.group(2) # This is the content as formatted by minidom
                closing_tag = match.group(3)

                # If minidom escaped characters like &lt;, they need to remain escaped if they were literal
                # However, for CDATA, the content should be exactly as is.
                # If 'content' was originally "<p>Hello</p>", minidom makes it "&lt;p&gt;Hello&lt;/p&gt;"
                # For CDATA, we want "<![CDATA[<p>Hello</p>]]>"
                # This is tricky. The current `clean_html_for_xml` prepares a string.
                # If that string is directly set as .text, minidom will escape it.
                # A better approach for CDATA is to ensure the ElementTree element's text
                # is the raw string intended for CDATA, and the XML writer handles it.
                # For now, we assume `content` is what should go inside CDATA.
                # We must avoid double-wrapping if by chance it's already CDATA.
                if content.strip().startswith("<![CDATA[") and content.strip().endswith("]]>"):
                    return match.group(0) 
                return f"{opening_tag}<![CDATA[{content}]]>{closing_tag}"
            
            temp_final_xml_string = pattern.sub(cdata_replacer, temp_final_xml_string)
    final_xml_string = temp_final_xml_string

    # Handle XML declaration
    if xml_structure_config.get('declaration', True):
        if not final_xml_string.strip().startswith('<?xml'):
            final_xml_string = '<?xml version="1.0" encoding="UTF-8"?>\n' + final_xml_string
        else:
            # Ensure the existing declaration is the one we want (e.g. UTF-8)
            final_xml_string = re.sub(
                r'<\?xml.*?\?>',
                '<?xml version="1.0" encoding="UTF-8"?>',
                final_xml_string,
                count=1, # Replace only the first instance
                flags=re.IGNORECASE 
            )
    else: # Remove declaration if explicitly set to false in config
        final_xml_string = re.sub(r'<\?xml.*?\?>\s*\n?', '', final_xml_string, count=1, flags=re.IGNORECASE).lstrip()
        
    return final_xml_string.encode('utf-8') # Return as bytes


def generate_structured_data_xml_content(doc_data, logger_instance):
    """
    Generates a string representation of structured data (e.g., from a CSV row)
    to be used if a generic <Content> tag needs to be populated for these types.
    Ideally, specific fields from doc_data are mapped to individual XML tags.
    """
    logger_instance.debug(f"Generating XML content string for structured data: {doc_data.get('Document_ID', 'N/A')}")
    content_lines = []
    
    # Exclude common metadata fields that are usually tagged separately or are internal
    excluded_keys = [
        "gcs_html_path", "pdf_gcs_paths", "xml_path", "processing_status", 
        "scrape_timestamp", "last_updated", "url_hash_identifier", 
        "indexed_filename_base", "Document_ID", "customer", "project",
        "document_type_hint_from_discovery", "pipeline_version", "xml_status",
        "xml_generated_timestamp", "xml_filename", "last_error", "is_csv_item",
        "original_source_file", "processed_csv_file", "source_csv_row_data",
        "main_url", "source_category_url", "source_page_url", "Document_Type" # These are often attributes or separate tags
    ] # Add more as needed

    title = doc_data.get('Document_Title', doc_data.get('title', 'Structured Data Item'))
    content_lines.append(f"Title: {title}")
    description = doc_data.get('Summary_Description', doc_data.get('description', ''))
    if description:
        content_lines.append(f"Description: {description}")

    for key, value in doc_data.items():
        if key not in excluded_keys and value is not None: # Ensure value is not None
            value_str = ", ".join(map(str, value)) if isinstance(value, list) else str(value)
            if value_str.strip(): # Add only if there's meaningful content
                content_lines.append(f"{key.replace('_', ' ').title()}: {value_str}") # Format key for readability
            
    if not content_lines: # Fallback if all fields were excluded or empty
        return f"Structured Data Item: {doc_data.get('Document_ID', 'N/A')}. No specific content fields available for generic representation."
        
    return "\n".join(content_lines)


def get_content_for_xml_field(doc_data, html_content_from_gcs_str, logger_instance):
    """
    Determines the appropriate string content for a generic XML content field (e.g., <Content>)
    based on the document type stored in doc_data.
    """
    document_type = doc_data.get("Document_Type", doc_data.get("document_type_hint_from_discovery", "HTML_DOCUMENT"))
    doc_id = doc_data.get('Document_ID', 'N/A')
    logger_instance.info(f"Getting content for generic XML field. Document ID: {doc_id}, Type: {document_type}")

    if document_type in ["FEDAO_MOA_ITEM", "FEDAO_TOA_ITEM", "STRUCTURED_DATA"]:
        # For FEDAO and generic structured data, generate a textual representation from its own fields.
        return generate_structured_data_xml_content(doc_data, logger_instance)
    
    elif document_type == "PDF_DOCUMENT":
        # For PDF documents, create a simple textual description.
        title = doc_data.get('Document_Title', doc_data.get('title', 'PDF Document'))
        description = doc_data.get('Summary_Description', doc_data.get('description', ''))
        pdf_paths = doc_data.get('pdf_gcs_paths', [])
        
        content_lines = [f"PDF Document Title: {title}"]
        if description:
            content_lines.append(f"Summary: {description}")
        if pdf_paths:
            content_lines.append("Associated PDF GCS Paths:")
            for pdf_path in pdf_paths:
                content_lines.append(f"  - {pdf_path}")
        return "\n".join(content_lines)
        
    elif document_type == "HTML_DOCUMENT":
        # For standard HTML_DOCUMENT, return the cleaned HTML content that was passed in.
        # This html_content_from_gcs_str should already have been processed by clean_html_for_xml.
        return html_content_from_gcs_str
        
    else: # Default or unknown types
        logger_instance.warning(f"Unhandled document type '{document_type}' for generic content generation. Returning basic info for ID {doc_id}.")
        return f"Document ID: {doc_id}. Type: '{document_type}'. Title: {doc_data.get('Document_Title', 'N/A')}"


@functions_framework.cloud_event
def generate_xml(cloud_event):
    active_logger = logger 
    pubsub_message = {}
    gcp_project_id_for_error = os.environ.get("GCP_PROJECT") # Fallback GCP ID
    publisher_for_error = None
    identifier = "unknown_identifier" # Fallback identifier
    doc_data_snapshot_for_error = {} 

    try:
        event_data = cloud_event.data
        if not event_data or 'message' not in event_data or 'data' not in event_data['message']:
            active_logger.error("Invalid Pub/Sub event structure: 'message' or 'data' field missing.")
            raise ValueError("Invalid Pub/Sub message structure.")

        pubsub_message_data = event_data['message']['data']
        pubsub_message = json.loads(base64.b64decode(pubsub_message_data).decode('utf-8'))
        
        customer_id = pubsub_message.get('customer')
        project_id_config_name = pubsub_message.get('project')
        identifier = pubsub_message.get('identifier') 
        date_str = pubsub_message.get('date', datetime.now(timezone.utc).strftime('%Y%m%d'))

        missing_fields = [f for f, v in {"customer": customer_id, "project": project_id_config_name, "identifier": identifier}.items() if not v]
        if missing_fields:
            error_msg = f"Missing required Pub/Sub fields: {', '.join(missing_fields)}"
            active_logger.error(error_msg) 
            raise ValueError(error_msg)

        active_logger = setup_logging(customer_id, project_id_config_name)
        active_logger.info(f"Starting generate_xml for identifier: {identifier}, project: {project_id_config_name}, date: {date_str}")

        customer_config = load_customer_config(customer_id)
        gcp_project_id = customer_config.get("gcp_project_id", os.environ.get("GCP_PROJECT"))
        if not gcp_project_id: # Override fallback if customer_config has it
             active_logger.error("GCP Project ID is not configured in customer config or environment variables.")
             raise ValueError("GCP Project ID not configured")
        gcp_project_id_for_error = gcp_project_id 
        
        # Initialize Firestore client for loading project configuration
        fs_db_id_cfg = customer_config.get('firestore_database_id_for_config', customer_config.get('firestore_database_id', '(default)'))
        db_opts_cfg = {"project": gcp_project_id}
        if fs_db_id_cfg != "(default)": db_opts_cfg["database"] = fs_db_id_cfg
        db_for_config = firestore.Client(**db_opts_cfg)
        
        project_config = load_dynamic_site_config(db_for_config, project_id_config_name, active_logger)

        gcs_bucket_name = project_config.get('gcs_bucket')
        firestore_collection_name = project_config.get('firestore_collection')
        fs_db_id_ops = project_config.get('firestore_database_id', fs_db_id_cfg) # Use project's DB ID, fallback to config's
        
        xml_structure_config = project_config.get('xml_structure')
        field_mappings = project_config.get('field_mappings', {})

        if not all([gcs_bucket_name, firestore_collection_name, xml_structure_config]):
            error_msg = "Configuration error: 'gcs_bucket', 'firestore_collection', or 'xml_structure' is missing."
            active_logger.error(error_msg)
            raise ValueError(error_msg)

        storage_client = storage.Client(project=gcp_project_id)
        gcs_bucket = storage_client.bucket(gcs_bucket_name)
        
        db_opts_ops = {"project": gcp_project_id}
        if fs_db_id_ops != "(default)": db_opts_ops["database"] = fs_db_id_ops
        db_operational = firestore.Client(**db_opts_ops)
        
        publisher_for_error = pubsub_v1.PublisherClient()

        doc_ref = db_operational.collection(firestore_collection_name).document(identifier)
        doc = None
        for attempt in range(MAX_FIRESTORE_RETRIES):
            try:
                doc = doc_ref.get()
                if doc.exists: break
                active_logger.warning(f"Firestore doc {identifier} not found (attempt {attempt + 1}). Retrying in {RETRY_BACKOFF * (2 ** attempt)}s...")
                time.sleep(RETRY_BACKOFF * (2 ** attempt))
            except Exception as e_fs_fetch:
                active_logger.warning(f"Firestore fetch for {identifier} failed (attempt {attempt + 1}): {str(e_fs_fetch)}")
                if attempt == MAX_FIRESTORE_RETRIES - 1:
                    active_logger.error(f"Max retries reached for Firestore fetch ({identifier}). Last error: {str(e_fs_fetch)}")
                    raise ValueError(f"Failed to fetch Firestore document {identifier} after retries: {str(e_fs_fetch)}")
                time.sleep(RETRY_BACKOFF * (2 ** attempt))
        
        if not doc or not doc.exists:
            active_logger.error(f"Firestore document {identifier} not found in collection '{firestore_collection_name}' after all retries.")
            return {'status': 'error_no_document', 'message': f'Firestore document {identifier} not found.'}

        doc_data = doc.to_dict()
        doc_data_snapshot_for_error = doc_data.copy() # For error reporting
        
        indexed_filename_base = doc_data.get("indexed_filename_base", identifier) 
        document_type = doc_data.get("Document_Type", doc_data.get("document_type_hint_from_discovery", "HTML_DOCUMENT"))
        active_logger.info(f"Processing document ID {identifier}, type: {document_type}. Filename base: {indexed_filename_base}")

        if doc_data.get('xml_path') and doc_data.get('xml_status') == 'Success': # Simpler check
            active_logger.info(f"XML already generated and marked as Success for {identifier} at {doc_data['xml_path']}. Skipping.")
            return {'status': 'skipped_existing', 'identifier': identifier, 'xml_path': doc_data['xml_path']}

        html_content_from_gcs_str = "" 
        if document_type == "HTML_DOCUMENT":
            html_gcs_path = doc_data.get('html_gcs_path')
            if not html_gcs_path or not html_gcs_path.startswith(f"gs://{gcs_bucket_name}/"):
                error_msg = f"Invalid/missing GCS HTML path for HTML_DOCUMENT {identifier}: {html_gcs_path}"
                active_logger.error(error_msg)
                doc_ref.update({"xml_status": f"Error: {error_msg}", "processing_stage": "xml_error_html_path_missing", "last_updated": firestore.SERVER_TIMESTAMP})
                raise ValueError(error_msg)
            try:
                blob_name = html_gcs_path.replace(f"gs://{gcs_bucket_name}/", "")
                html_blob = gcs_bucket.blob(blob_name)
                if not html_blob.exists(): raise FileNotFoundError(f"HTML blob {blob_name} not in GCS for {identifier}.")
                
                dl_bytes = html_blob.download_as_bytes()
                html_content_from_gcs_str = clean_html_for_xml(gzip.decompress(dl_bytes) if blob_name.endswith('.gz') else dl_bytes, active_logger)
                
                if not html_content_from_gcs_str and project_config.get('xml_structure',{}).get('require_html_content_for_html_doc', True):
                    raise ValueError("Cleaned HTML content is empty for HTML_DOCUMENT, but required by config.")
                active_logger.info(f"Successfully processed HTML for {identifier} from {html_gcs_path}")
            except Exception as e_gcs:
                error_msg = f"Failed to get/process HTML from GCS for {identifier} ({html_gcs_path}): {str(e_gcs)}"
                active_logger.error(error_msg, exc_info=True)
                doc_ref.update({"xml_status": f"Error: {sanitize_error_message(error_msg)}", "processing_stage": "xml_error_gcs_html_download", "last_updated": firestore.SERVER_TIMESTAMP})
                raise
        
        # This generic_content_for_xml is for any field configured with source "document_content_auto"
        generic_content_for_xml = get_content_for_xml_field(doc_data, html_content_from_gcs_str, active_logger)

        xml_root_tag = xml_structure_config.get('root_tag', 'document')
        root_element = Element(xml_root_tag)
        root_element.set('document_type', document_type)
        if doc_data.get('source_category_url'): root_element.set('source_category_url', str(doc_data.get('source_category_url')))
        if doc_data.get('source_page_url'): root_element.set('source_page_url', str(doc_data.get('source_page_url')))

        for pi_target, pi_text in xml_structure_config.get('processing_instructions', []):
            root_element.append(ProcessingInstruction(pi_target, pi_text))

        missing_fields_log = []
        doc_data_for_mapping = {**doc_data, "indexed_filename_base": indexed_filename_base}

        for field_conf in xml_structure_config.get('fields', []):
            tag_name = field_conf.get('tag') or field_conf.get('target_tag')
            source_key = field_conf.get('source')
            if not tag_name or not source_key: continue

            field_value_str = ""
            if source_key in ["gcs_html_content", "document_content_auto"]: # Standard keys for main content
                field_value_str = generic_content_for_xml
            else:
                raw_value = get_mapped_field(doc_data_for_mapping, source_key, field_mappings, active_logger)
                if raw_value is None or (isinstance(raw_value, str) and raw_value == "Not Available"):
                    field_value_str = field_conf.get('default_value', '')
                    if field_conf.get('required', False) and not field_value_str:
                        error_msg = f"Required XML field '{tag_name}' (source: '{source_key}') missing for {identifier}."
                        active_logger.error(error_msg)
                        doc_ref.update({"xml_status": f"Error: {sanitize_error_message(error_msg)}", "processing_stage": "xml_error_missing_required_field", "last_updated": firestore.SERVER_TIMESTAMP})
                        raise ValueError(error_msg)
                    missing_fields_log.append(f"{tag_name} (from {source_key}, used default)")
                elif isinstance(raw_value, bool): field_value_str = str(raw_value).lower()
                elif isinstance(raw_value, datetime): field_value_str = raw_value.isoformat()
                elif isinstance(raw_value, list): field_value_str = ", ".join(map(str, raw_value))
                elif isinstance(raw_value, firestore.SERVER_TIMESTAMP.__class__): field_value_str = datetime.now(timezone.utc).isoformat()
                else: field_value_str = str(raw_value)
            
            sub_element = SubElement(root_element, tag_name)
            sub_element.text = field_value_str

            for attr_name, attr_source in field_conf.get('attributes', {}).items():
                attr_val = get_mapped_field(doc_data_for_mapping, attr_source, field_mappings, active_logger)
                if attr_val is not None and attr_val != "Not Available": sub_element.set(attr_name, str(attr_val))
        
        if missing_fields_log: active_logger.info(f"XML for {identifier}: Defaults used for {', '.join(missing_fields_log)}")

        xml_bytes_output = custom_pretty_xml(root_element, xml_structure_config)
        
        xml_final_filename = sanitize_xml_filename_from_base(indexed_filename_base, xml_structure_config, active_logger)
        proj_id_sanitized = "".join(c if c.isalnum() else '_' for c in project_id_config_name)
        gcs_xml_path_tmpl = xml_structure_config.get('gcs_xml_output_path', f"xml_output/{proj_id_sanitized}/{{date_str}}")
        xml_gcs_folder = gcs_xml_path_tmpl.format(project_id_sanitized=proj_id_sanitized, date_str=date_str)
        xml_dest_gcs_path = f"{xml_gcs_folder.rstrip('/')}/{xml_final_filename}"
        
        xml_blob = gcs_bucket.blob(xml_dest_gcs_path)
        # Retry logic for GCS upload
        for attempt in range(MAX_GCS_RETRIES):
            try:
                xml_blob.upload_from_string(xml_bytes_output, content_type='application/xml; charset=utf-8')
                active_logger.info(f"Saved XML for {identifier} (type: {document_type}) to gs://{gcs_bucket_name}/{xml_dest_gcs_path}")
                break 
            except Exception as e_upload:
                active_logger.warning(f"GCS XML upload attempt {attempt + 1}/{MAX_GCS_RETRIES} for {identifier} failed: {e_upload}")
                if attempt == MAX_GCS_RETRIES - 1:
                    doc_ref.update({"xml_status": f"Error: Max GCS upload retries - {sanitize_error_message(str(e_upload))}", "processing_stage": "xml_error_gcs_upload", "last_updated": firestore.SERVER_TIMESTAMP})
                    raise Exception(f"Failed to upload XML to GCS for {identifier} after {MAX_GCS_RETRIES} retries: {e_upload}")
                time.sleep(RETRY_BACKOFF * (2**attempt))
        
        final_xml_gcs_uri = f"gs://{gcs_bucket_name}/{xml_dest_gcs_path}"
        fs_update_data = {
            'xml_path': final_xml_gcs_uri, 'xml_filename': xml_final_filename,
            'xml_status': 'Success', 'xml_generated_timestamp': firestore.SERVER_TIMESTAMP,
            'processing_stage': 'xml_generated', 'last_updated': firestore.SERVER_TIMESTAMP,
            'last_error': firestore.DELETE_FIELD
        }
        # Retry logic for Firestore update
        for attempt in range(MAX_FIRESTORE_RETRIES):
            try:
                doc_ref.update(fs_update_data)
                active_logger.info(f"Updated Firestore for {identifier} with XML success status.")
                break
            except Exception as e_fs_update:
                active_logger.warning(f"Firestore XML status update attempt {attempt + 1}/{MAX_FIRESTORE_RETRIES} for {identifier} failed: {e_fs_update}")
                if attempt == MAX_FIRESTORE_RETRIES - 1:
                    active_logger.error(f"Max retries for Firestore XML status update for {identifier}. XML is at {final_xml_gcs_uri} but DB status is not updated.")
                    # Critical: XML generated but status not updated. May need manual intervention or specific retry for this update.
                    raise Exception(f"Failed to update Firestore status for {identifier} after XML generation: {e_fs_update}")
                time.sleep(RETRY_BACKOFF * (2**attempt))

        if project_config.get("trigger_generate_reports_after_xml", True):
            report_payload = {
                'customer': customer_id, 'project': project_id_config_name, 'identifier': identifier,
                'status': 'xml_generation_completed', 'date': date_str,
                'xml_output_path': final_xml_gcs_uri, 'document_type': document_type
            }
            reports_topic = project_config.get("pubsub_topics", {}).get("generate_reports", NEXT_STEP_GENERATE_REPORTS_TOPIC_NAME)
            publisher_for_error.publish(publisher_for_error.topic_path(gcp_project_id, reports_topic), json.dumps(report_payload).encode('utf-8'))
            active_logger.info(f"Published to '{reports_topic}' for {identifier} after XML gen.")

        return {'status': 'success', 'identifier': identifier, 'xml_path': final_xml_gcs_uri, 'document_type': document_type}

    except Exception as e_critical:
        active_logger.error(f"CRITICAL error in generate_xml for identifier '{identifier}': {str(e_critical)}", exc_info=True)
        if gcp_project_id_for_error and publisher_for_error:
            retry_payload = {
                "customer": pubsub_message.get("customer", "unknown_customer"),
                "project": pubsub_message.get("project", "unknown_project"),
                "original_pubsub_message": pubsub_message,
                "error_message": sanitize_error_message(str(e_critical)),
                "stage": "generate_xml_critical",
                "retry_count": pubsub_message.get("retry_count", 0) + 1,
                "identifier_in_error": identifier,
                "doc_data_snapshot": serialize_firestore_doc(doc_data_snapshot_for_error) if doc_data_snapshot_for_error else "Doc data not captured"
            }
            try:
                topic_path_retry = publisher_for_error.topic_path(gcp_project_id_for_error, RETRY_TOPIC_NAME)
                publisher_for_error.publish(topic_path_retry, json.dumps(retry_payload, default=str).encode('utf-8'))
                active_logger.info(f"Published critical error details for {identifier} to '{RETRY_TOPIC_NAME}'.")
            except Exception as e_retry_pub:
                active_logger.error(f"Failed to publish critical error for {identifier} to retry topic '{RETRY_TOPIC_NAME}': {str(e_retry_pub)}")
        else:
            active_logger.error("Cannot publish critical error to retry topic: GCP Project ID or Publisher client was not initialized.")
        raise # Re-raise to ensure Cloud Function execution is marked as failed