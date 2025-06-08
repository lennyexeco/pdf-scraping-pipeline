# src/functions/extract_initial_metadata/main.py

import json
import base64
import logging
import os
import time
import re
import hashlib
from datetime import datetime, timezone
from urllib.parse import urlparse, unquote

import requests
import functions_framework
from google.cloud import firestore, pubsub_v1
from google.api_core.exceptions import Aborted, DeadlineExceeded, ServiceUnavailable

# Conditional import for Vertex AI, only if AI features are used for non-CSV items
AI_ENABLED_PROJECT_WIDE = os.environ.get("AI_METADATA_EXTRACTION_PROJECT_WIDE_ENABLED", "false").lower() == "true"
if AI_ENABLED_PROJECT_WIDE:
    try:
        import vertexai
        from vertexai.generative_models import GenerativeModel, Part, GenerationConfig, HarmCategory, HarmBlockThreshold
    except ImportError:
        logging.getLogger(__name__).warning("Vertex AI SDK not installed. AI features will be disabled.")
        AI_ENABLED_PROJECT_WIDE = False

from src.common.utils import generate_url_hash, setup_logging
from src.common.config import load_customer_config, load_dynamic_site_config
from src.common.helpers import sanitize_field_name

# Initialize logger at the module level
logger = logging.getLogger(__name__)

# Constants
NEXT_STEP_FETCH_CONTENT_TOPIC_NAME = "fetch-content-topic"
NEXT_STEP_GENERATE_XML_TOPIC_NAME = "generate-xml-topic"
MAX_HTML_FOR_PROMPT = int(os.environ.get("MAX_HTML_FOR_PROMPT", 3000000))
MAX_TRANSACTION_RETRIES = 5
INITIAL_RETRY_DELAY_SECONDS = 1
DEFAULT_REQUESTS_TIMEOUT = 20

# --- Helper Functions ---
def _get_next_id_tx(transaction_obj, counter_ref_arg, project_abbreviation_arg, active_logger_instance):
    """Transactional function to get the next sequence ID."""
    active_logger = active_logger_instance or logging.getLogger(__name__)
    
    result = transaction_obj.get(counter_ref_arg)
    counter_snapshot = None

    if isinstance(result, firestore.DocumentSnapshot):
        counter_snapshot = result
    elif hasattr(result, '__iter__'):
        try:
            counter_snapshot = next(result, None)
            if counter_snapshot and next(result, None) is not None:
                active_logger.warning(f"Transaction get for {counter_ref_arg.path} unexpectedly returned multiple docs. Using first.")
        except StopIteration:
             pass
        except Exception as e_iter:
            active_logger.error(f"Error iterating result from transaction.get for {counter_ref_arg.path}: {e_iter}")
    else:
        active_logger.warning(f"Transaction get for {counter_ref_arg.path} returned unexpected type: {type(result)}.")

    current_sequence = 0
    if counter_snapshot and counter_snapshot.exists:
        current_sequence = counter_snapshot.to_dict().get("current_sequence", 0)
    
    next_sequence = current_sequence + 1
    transaction_obj.set(counter_ref_arg, {"current_sequence": next_sequence, "last_updated": firestore.SERVER_TIMESTAMP})
    return f"{project_abbreviation_arg}_{next_sequence}"

def generate_sequential_document_id(db, project_config, project_abbreviation, main_url_for_fallback, active_logger_instance):
    """Generates a sequential Document ID or falls back to a hash-based one."""
    active_logger = active_logger_instance or logging.getLogger(__name__)
    
    sequential_id_config = project_config.get("sequential_id_config", {})
    if not sequential_id_config.get("enabled", False):
        active_logger.info(f"Sequential ID generation not enabled for '{project_abbreviation}'. Using fallback.")
        now_utc = datetime.now(timezone.utc)
        ts_str = now_utc.strftime("%Y%m%d_%H%M%S_%f")[:-3]
        fallback_hash_input = str(main_url_for_fallback) if main_url_for_fallback else os.urandom(16)
        url_hash_fb = hashlib.md5(str(fallback_hash_input).encode('utf-8')).hexdigest()[:8]
        return f"{project_abbreviation}_FB_{ts_str}_{url_hash_fb}"

    counters_collection = sequential_id_config.get("firestore_counters_collection", "doc_counters")
    counter_doc_id = sequential_id_config.get("counter_doc_prefix", f"{project_abbreviation}_id_sequence")
    counter_doc_ref = db.collection(counters_collection).document(counter_doc_id)
    
    new_doc_id = None
    retries = 0
    current_delay = INITIAL_RETRY_DELAY_SECONDS

    @firestore.transactional 
    def _transactional_update_wrapper(transaction, ref, abbr):
        return _get_next_id_tx(transaction, ref, abbr, active_logger)

    while retries < MAX_TRANSACTION_RETRIES:
        try:
            new_doc_id = db.run_transaction(_transactional_update_wrapper, counter_doc_ref, project_abbreviation)
            active_logger.info(f"Generated sequential Document ID: {new_doc_id} (Attempt {retries + 1})")
            return new_doc_id
        except (Aborted, DeadlineExceeded, ServiceUnavailable) as e_trans:
            active_logger.warning(f"Transaction for sequential ID failed (Attempt {retries + 1}/{MAX_TRANSACTION_RETRIES}) with {type(e_trans).__name__}: {e_trans}. Retrying in {current_delay}s...")
            time.sleep(current_delay)
            retries += 1
            current_delay = min(current_delay * 2, 30)
        except Exception as e_fatal:
            active_logger.error(f"Fatal error during sequential ID transaction: {e_fatal}", exc_info=True)
            break

    active_logger.error(f"All retries for sequential ID generation failed for '{project_abbreviation}'. Falling back.")
    now_utc = datetime.now(timezone.utc)
    ts_str = now_utc.strftime("%Y%m%d_%H%M%S_%f")[:-3]
    fallback_hash_input = str(main_url_for_fallback) if main_url_for_fallback else os.urandom(16)
    url_hash_fb = hashlib.md5(str(fallback_hash_input).encode('utf-8')).hexdigest()[:8]
    return f"{project_abbreviation}_FB_RETRYFAIL_{ts_str}_{url_hash_fb}"

def extract_date_from_fedao_csv_data(metadata_dict, active_logger_instance=None):
    """Extract date from FEDAO CSV data - handles both operation dates and source dates"""
    active_logger = active_logger_instance or logging.getLogger(__name__)
    
    # Priority order for date fields in FEDAO data (updated for new format)
    date_keys_priority = ['Source_Date', 'OPERATION DATE', 'DATE', 'Operation_Date', 'Release_Date']
    
    for key in date_keys_priority:
        date_val = metadata_dict.get(key)
        if date_val:
            # Handle YYYYMMDD format (Source_Date)
            if isinstance(date_val, str) and re.match(r"^\d{8}$", date_val):
                try:
                    dt_obj = datetime.strptime(date_val, '%Y%m%d')
                    return dt_obj.strftime('%Y-%m-%d')
                except ValueError:
                    active_logger.debug(f"Could not parse YYYYMMDD date '{date_val}'")
            
            # Handle ISO format dates (YYYY-MM-DDTHH:MM:SS.000Z)
            if isinstance(date_val, str) and 'T' in date_val:
                try:
                    dt_obj = datetime.fromisoformat(date_val.replace('Z', '+00:00'))
                    return dt_obj.strftime('%Y-%m-%d')
                except ValueError:
                    active_logger.debug(f"Could not parse ISO date '{date_val}'")
            
            # Handle standard date formats
            if isinstance(date_val, str):
                for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%B %d, %Y', '%b %d, %Y']:
                    try:
                        dt_obj = datetime.strptime(date_val.strip(), fmt)
                        return dt_obj.strftime('%Y-%m-%d')
                    except ValueError:
                        continue
    
    active_logger.info("No valid date found in FEDAO CSV metadata. Using current date.")
    return datetime.now(timezone.utc).strftime('%Y-%m-%d')

def classify_fedao_document_type(csv_row_data, type_hint, active_logger_instance=None):
    """Classify FEDAO document type based on CSV data structure"""
    active_logger = active_logger_instance or logging.getLogger(__name__)
    
    # Explicit hints from CSV processing take precedence
    if type_hint in ["FEDAO_MOA_ITEM", "FEDAO_TOA_ITEM"]:
        active_logger.info(f"Classified as '{type_hint}' based on hint.")
        return type_hint
    
    # Determine type based on available columns
    if 'OPERATION DATE' in csv_row_data and 'SETTLEMENT DATE' in csv_row_data:
        return "FEDAO_MOA_ITEM"
    elif 'DATE' in csv_row_data and 'CUSIP' in csv_row_data:
        return "FEDAO_TOA_ITEM"
    
    # Check for legacy column names
    if 'MAXIMUMOPERATIONSIZE' in csv_row_data or 'MAXIMUM_OPERATION_SIZE_VALUE' in csv_row_data:
        return "FEDAO_MOA_ITEM"
    elif 'MAXIMUM PURCHASE AMOUNT' in csv_row_data:
        return "FEDAO_TOA_ITEM"
    
    active_logger.info("Could not determine specific FEDAO type. Using generic classification.")
    return "FEDAO_ITEM"

def generate_fedao_title(csv_row_data, doc_type, active_logger_instance=None):
    """Generate meaningful title for FEDAO items"""
    active_logger = active_logger_instance or logging.getLogger(__name__)
    
    if doc_type == "FEDAO_MOA_ITEM":
        # For MOA items: Operation Type + Security Type + Date
        operation_type = csv_row_data.get("OPERATION TYPE", "")
        security_type = csv_row_data.get("SECURITY TYPE AND MATURITY", "")
        operation_date = csv_row_data.get("OPERATION DATE", "")
        
        title_parts = []
        if operation_type:
            title_parts.append(operation_type)
        if security_type:
            title_parts.append(security_type)
        if operation_date and 'T' in operation_date:
            # Extract date part from ISO format
            try:
                dt_obj = datetime.fromisoformat(operation_date.replace('Z', '+00:00'))
                title_parts.append(dt_obj.strftime('%Y-%m-%d'))
            except:
                pass
        
        return " - ".join(title_parts) if title_parts else "FEDAO MOA Operation"
    
    elif doc_type == "FEDAO_TOA_ITEM":
        # For TOA items: Operation Type + CUSIP + Date
        operation_type = csv_row_data.get("OPERATION TYPE", "")
        cusip = csv_row_data.get("CUSIP", "")
        date_val = csv_row_data.get("DATE", "")
        
        title_parts = []
        if operation_type:
            title_parts.append(operation_type)
        if cusip:
            title_parts.append(f"CUSIP: {cusip}")
        if date_val:
            if 'T' in date_val:
                try:
                    dt_obj = datetime.fromisoformat(date_val.replace('Z', '+00:00'))
                    title_parts.append(dt_obj.strftime('%Y-%m-%d'))
                except:
                    title_parts.append(date_val)
            else:
                title_parts.append(date_val)
        
        return " - ".join(title_parts) if title_parts else "FEDAO TOA Operation"
    
    return "FEDAO Operation"

def extract_title_from_url(url_str, active_logger_instance=None):
    """Extract title from URL for web items"""
    active_logger = active_logger_instance or logging.getLogger(__name__)
    try:
        parsed_url = urlparse(str(url_str))
        path_part = unquote(parsed_url.path)
        filename = os.path.basename(path_part)
        if not filename and parsed_url.hostname:
            filename = parsed_url.hostname
        
        name_without_ext = os.path.splitext(filename)[0]
        title_slug = name_without_ext.replace('-', ' ').replace('_', ' ').replace('.', ' ')
        title_parts = [word.capitalize() for word in title_slug.split() if word]
        
        extracted_title = ' '.join(title_parts)
        return extracted_title if extracted_title else "Untitled Document"
    except Exception as e:
        active_logger.warning(f"Could not extract title from URL '{url_str}': {e}")
        return "Untitled Document"

def extract_date_from_various_sources(metadata_dict, active_logger_instance=None):
    """Extract date from various metadata sources for web items"""
    active_logger = active_logger_instance or logging.getLogger(__name__)
    
    # Prioritize specific date fields
    date_keys_priority = ['Source_Date', 'Release_Date', 'Publication_Date', 'Date', 'publishDate', 'datePublished']
    for key in date_keys_priority:
        date_val = metadata_dict.get(key)
        if date_val:
            # Try parsing YYYY-MM-DD directly
            if isinstance(date_val, str) and re.match(r"^\d{4}-\d{2}-\d{2}$", date_val):
                active_logger.info(f"Using pre-formatted date '{date_val}' from key '{key}'.")
                return date_val
            
            # Handle YYYYMMDD format
            if isinstance(date_val, str) and re.match(r"^\d{8}$", date_val):
                try:
                    dt_obj = datetime.strptime(date_val, '%Y%m%d')
                    return dt_obj.strftime('%Y-%m-%d')
                except ValueError:
                    pass
            
            # Handle datetime objects
            if isinstance(date_val, datetime):
                return date_val.strftime('%Y-%m-%d')

    # Fallback: Search for dates in text fields
    text_to_search = ""
    for key in ['description', 'summary', 'content', 'full_text', 'title']:
        text_to_search += str(metadata_dict.get(key, "")) + " "
    
    # Regex for YYYY-MM-DD
    match_yyyy_mm_dd = re.search(r'\b(20\d{2}-\d{2}-\d{2})\b', text_to_search)
    if match_yyyy_mm_dd:
        active_logger.info(f"Extracted date '{match_yyyy_mm_dd.group(1)}' via regex from text content.")
        return match_yyyy_mm_dd.group(1)

    # Regex for MM/DD/YYYY
    match_mm_dd_yyyy = re.search(r'\b(\d{1,2}/\d{1,2}/(20\d{2}))\b', text_to_search)
    if match_mm_dd_yyyy:
        try:
            dt_obj = datetime.strptime(match_mm_dd_yyyy.group(1), '%m/%d/%Y')
            formatted_date = dt_obj.strftime('%Y-%m-%d')
            active_logger.info(f"Extracted and converted date '{formatted_date}' from MM/DD/YYYY format.")
            return formatted_date
        except ValueError:
            active_logger.debug(f"Could not convert '{match_mm_dd_yyyy.group(1)}' from MM/DD/YYYY format.")

    active_logger.info("No specific date found in metadata or text. Defaulting to current UTC date.")
    return datetime.now(timezone.utc).strftime('%Y-%m-%d')

def classify_document_type(doc_title, doc_url, type_hint, project_cfg, active_logger_instance=None):
    """Classify document type for web items"""
    active_logger = active_logger_instance or logging.getLogger(__name__)
    
    # Explicit hints from CSV processing take precedence
    if type_hint in ["FEDAO_MOA_ITEM", "FEDAO_TOA_ITEM"]:
        active_logger.info(f"Classified as '{type_hint}' based on hint.")
        return type_hint

    # Basic rule: PDF extension
    if doc_url and str(doc_url).lower().endswith('.pdf'):
        active_logger.info("Classified as 'PDF Document' based on URL extension.")
        return "PDF Document"
    
    # Hint for generic structured data (non-FEDAO)
    if type_hint == "STRUCTURED_DATA":
        active_logger.info("Classified as 'Structured Data' based on hint (non-FEDAO).")
        return "Structured Data"
        
    # Default for web pages
    active_logger.info("Defaulting document type to 'HTML Document'.")
    return "HTML_DOCUMENT"

# AI-related functions (simplified for FEDAO focus)
def validate_html_content(html_bytes, active_logger_instance=None):
    """Validate HTML content for AI processing"""
    active_logger = active_logger_instance or logging.getLogger(__name__)
    if not html_bytes or len(html_bytes) < 100:
        active_logger.warning("HTML content is too short or empty for AI processing.")
        return None
    try:
        content_str = html_bytes.decode('utf-8', errors='ignore')
        if not ("<html" in content_str.lower() and "</html" in content_str.lower()):
            active_logger.warning("Content does not appear to be valid HTML for AI processing.")
            return None
        return content_str[:MAX_HTML_FOR_PROMPT]
    except Exception as e:
        active_logger.error(f"Error validating/decoding HTML for AI: {e}")
        return None

def invoke_vertex_ai_for_metadata(html_content_str, project_config, active_logger_instance=None):
    """Invoke Vertex AI for metadata extraction (placeholder for web items)"""
    active_logger = active_logger_instance or logging.getLogger(__name__)
    if not AI_ENABLED_PROJECT_WIDE:
        active_logger.info("Vertex AI metadata extraction is disabled project-wide.")
        return {}

    ai_config = project_config.get("ai_metadata_extraction", {})
    if not ai_config.get("enabled", False):
        active_logger.info("Vertex AI metadata extraction disabled in project_config.")
        return {}

    # For FEDAO, AI is typically not needed as data is structured
    # This is a placeholder implementation
    return {}

# --- Main Cloud Function ---
@functions_framework.cloud_event
def extract_initial_metadata(cloud_event):
    """Updated metadata extraction for FEDAO format compliance"""
    
    global logger
    if not logger.hasHandlers():
        log_level_env = os.environ.get("LOG_LEVEL", "INFO").upper()
        logging.basicConfig(level=log_level_env, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Initialize variables
    pubsub_message = {}
    customer_id = "unknown_customer"
    project_config_name_from_msg = "unknown_project"
    main_url_from_msg = "unknown_url"
    message_id_from_event = "N/A"
    active_logger = logger

    try:
        # 1. Parse Pub/Sub Message
        if not (cloud_event.data and "message" in cloud_event.data and "data" in cloud_event.data["message"]):
            active_logger.error(f"Invalid CloudEvent structure: {cloud_event.data}")
            return {"status": "error_bad_event_structure", "message": "Invalid CloudEvent structure"}
        
        message_id_from_event = cloud_event.data["message"].get("messageId", "N/A")
        pubsub_message_b64 = cloud_event.data["message"]["data"]
        pubsub_message_str = base64.b64decode(pubsub_message_b64).decode("utf-8")
        pubsub_message = json.loads(pubsub_message_str)

        customer_id = pubsub_message.get("customer")
        project_config_name_from_msg = pubsub_message.get("project")
        main_url_from_msg = pubsub_message.get("main_url")

        # 2. Setup Contextual Logging & Load Configs
        if not all([customer_id, project_config_name_from_msg, main_url_from_msg]):
            active_logger.error(f"Missing critical fields in Pub/Sub message: customer='{customer_id}', project='{project_config_name_from_msg}', main_url='{main_url_from_msg}'")
            raise ValueError("Pub/Sub message missing required fields")

        active_logger = setup_logging(customer_id, project_config_name_from_msg)
        active_logger.info(f"Processing FEDAO message: {message_id_from_event} for {main_url_from_msg}")

        customer_config = load_customer_config(customer_id, active_logger_instance=active_logger)
        gcp_project_id = customer_config.get("gcp_project_id", os.environ.get("GCP_PROJECT"))
        
        if not gcp_project_id:
            active_logger.error("GCP Project ID not configured")
            raise ValueError("GCP Project ID configuration missing")

        db = firestore.Client(project=gcp_project_id, database=customer_config.get("firestore_database_id", "(default)"))
        project_config = load_dynamic_site_config(db, project_config_name_from_msg, active_logger_instance=active_logger)
        
        project_abbreviation = project_config.get("project_abbreviation", "FEDAO")
        firestore_collection = project_config.get("firestore_collection", "fedao_documents")
        
        # 3. Process CSV Item (FEDAO specific)
        is_csv_item_source = pubsub_message.get("is_csv_item", False)
        discovered_metadata = pubsub_message.get("document_metadata", {})
        
        if not is_csv_item_source:
            # Handle web items (non-CSV) - simplified for FEDAO focus
            active_logger.info("Processing web item (non-CSV)")
            return handle_web_item(pubsub_message, db, project_config, active_logger)
        
        # Generate document ID
        final_doc_id = generate_sequential_document_id(db, project_config, project_abbreviation, main_url_from_msg, active_logger)
        url_hash_id = hashlib.md5(final_doc_id.encode()).hexdigest()[:16]
        
        # 4. Process FEDAO CSV Item
        active_logger.info(f"Processing FEDAO CSV Item: {final_doc_id}")
        
        csv_row_map = discovered_metadata
        
        # Classify document type
        doc_type_hint = pubsub_message.get("document_type", "FEDAO_ITEM")
        doc_type = classify_fedao_document_type(csv_row_map, doc_type_hint, active_logger)
        
        # Generate document title
        doc_title = generate_fedao_title(csv_row_map, doc_type, active_logger)
        
        # Extract and format release date (now using Source_Date)
        release_date_str = extract_date_from_fedao_csv_data(csv_row_map, active_logger)
        year_str = release_date_str.split('-')[0] if release_date_str else datetime.now(timezone.utc).strftime('%Y')
        
        # Build base Firestore payload
        base_firestore_payload = {
            "main_url": main_url_from_msg,
            "url_hash_identifier": url_hash_id,
            "Document_ID": final_doc_id,
            "Document_Title": doc_title,
            "Document_Type": doc_type,
            "Release_Date": release_date_str,
            "Year": year_str,
            "processing_status": "csv_item_metadata_complete",
            "scrape_timestamp": firestore.SERVER_TIMESTAMP,
            "last_updated": firestore.SERVER_TIMESTAMP,
            "pipeline_version": project_config.get("pipeline_version_tag", "1.0.1"),
            "customer_id": customer_id,
            "project_name": project_config_name_from_msg,
            "is_csv_item_source": True,
            "requires_html_scraping": False,
            "ai_metadata_extraction_successful": False
        }
        
        # Apply field mappings for FEDAO-specific fields
        for fs_key, map_cfg in project_config.get("field_mappings", {}).items():
            if isinstance(map_cfg, dict) and "source" in map_cfg:
                source_cols_str = map_cfg["source"]
                source_col_options = [s.strip() for s in source_cols_str.split("||")]
                
                for col_opt in source_col_options:
                    if col_opt in csv_row_map and csv_row_map[col_opt] and str(csv_row_map[col_opt]).strip():
                        # Handle numeric fields specially
                        if fs_key == "Maximum_Operation_Size" and col_opt == "MAXIMUMOPERATIONSIZE":
                            try:
                                # Ensure numeric value is stored properly
                                base_firestore_payload[fs_key] = float(csv_row_map[col_opt])
                            except (ValueError, TypeError):
                                base_firestore_payload[fs_key] = str(csv_row_map[col_opt]).strip()
                        else:
                            base_firestore_payload[fs_key] = str(csv_row_map[col_opt]).strip()
                        break
                else:
                    base_firestore_payload[fs_key] = ""
        
        # Add FEDAO-specific metadata
        fedao_defaults = project_config.get("fedao_config", {})
        base_firestore_payload["Topics"] = fedao_defaults.get("default_topics", ["Federal Reserve Operations"])
        base_firestore_payload["Key_Legislation_Mentioned"] = fedao_defaults.get("default_legislation", ["Federal Reserve Act"])
        base_firestore_payload["Summary_Description"] = doc_title
        
        # Store to Firestore
        doc_ref = db.collection(firestore_collection).document(final_doc_id)
        doc_ref.set(base_firestore_payload, merge=True)
        active_logger.info(f"Stored FEDAO metadata for {final_doc_id}: '{doc_title}'")
        
        return {
            "status": "success_fedao_csv_item_stored",
            "identifier": final_doc_id,
            "document_type_classified": doc_type,
            "document_title": doc_title
        }
        
    except ValueError as ve:
        active_logger.error(f"Configuration error: {str(ve)}", exc_info=True)
        return {"status": "error_value", "message": str(ve), "message_id": message_id_from_event}
    except Exception as e_main:
        active_logger.error(f"Critical error: {str(e_main)}", exc_info=True)
        return {"status": "error_critical_exception", "message": f"Unexpected error: {str(e_main)}", "message_id": message_id_from_event}

def handle_web_item(pubsub_message, db, project_config, active_logger):
    """Handle web items (non-CSV) - simplified implementation for FEDAO focus"""
    active_logger.info("Web item processing - simplified for FEDAO focus")
    
    # For FEDAO, web items are minimal since focus is on CSV operations
    # This is a placeholder that can be expanded if needed
    
    main_url = pubsub_message.get("main_url", "")
    customer_id = pubsub_message.get("customer", "simba")
    project_name = pubsub_message.get("project", "fedao_project")
    
    # Generate a simple document ID
    now_utc = datetime.now(timezone.utc)
    ts_str = now_utc.strftime("%Y%m%d_%H%M%S")
    web_doc_id = f"FEDAO_WEB_{ts_str}"
    
    # Basic metadata for web items
    web_payload = {
        "main_url": main_url,
        "Document_ID": web_doc_id,
        "Document_Title": extract_title_from_url(main_url, active_logger),
        "Document_Type": "WEB_ITEM",
        "processing_status": "web_item_basic_metadata",
        "scrape_timestamp": firestore.SERVER_TIMESTAMP,
        "customer_id": customer_id,
        "project_name": project_name,
        "is_csv_item_source": False
    }
    
    # Store basic web item metadata
    firestore_collection = project_config.get("firestore_collection", "fedao_documents")
    doc_ref = db.collection(firestore_collection).document(web_doc_id)
    doc_ref.set(web_payload, merge=True)
    
    active_logger.info(f"Stored basic web item metadata: {web_doc_id}")
    
    return {
        "status": "success_web_item_basic",
        "identifier": web_doc_id,
        "document_type_classified": "WEB_ITEM"
    }