import json
import base64
import logging
import os
import time
import re
import hashlib
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
import functions_framework # For Cloud Function decorator
from google.cloud import firestore, pubsub_v1
from google.cloud.firestore_v1.base_query import FieldFilter # Added for specific queries if needed later
from google.api_core.exceptions import Aborted, DeadlineExceeded, ServiceUnavailable # For retry logic

import vertexai
from vertexai.generative_models import GenerativeModel, Part, GenerationConfig

# Assuming these are correctly placed relative to your Cloud Function's main.py
from src.common.utils import generate_url_hash, setup_logging
from src.common.config import load_customer_config, load_dynamic_site_config
from src.common.helpers import (
    validate_html_content,
    sanitize_field_name # Ensure this helper is available and used if needed for field names
)

# --- Global Configuration ---
logger = logging.getLogger(__name__) # Standard logger
NEXT_STEP_FETCH_CONTENT_TOPIC_NAME = "fetch-content-topic"
NEXT_STEP_GENERATE_XML_TOPIC_NAME = "generate-xml-topic"
MAX_HTML_FOR_PROMPT = 3000000
MAX_TRANSACTION_RETRIES = 5 # Max retries for Firestore transaction
INITIAL_RETRY_DELAY_SECONDS = 1 # Initial delay for retrying transaction


# --- New Document ID Generation with Retry ---
def _get_next_id_tx(transaction_obj, counter_ref_arg, project_abbreviation_arg):
    """
    Transactional function to get the next sequence number.
    'transaction_obj' is passed by the transactional decorator/runner.
    """
    # Attempt to get the document snapshot(s)
    result = transaction_obj.get(counter_ref_arg)
    
    # Check if the result is a generator (as the error suggests)
    # If it's a generator, try to get the first item.
    # If it's already a DocumentSnapshot, this logic will still work.
    counter_snapshot = None
    if hasattr(result, '__iter__') and not isinstance(result, firestore.DocumentSnapshot): # Check if it's iterable but not already a snapshot
        try:
            counter_snapshot = next(result, None) # Get the first item, or None if generator is empty
            # You might want to log if there's more than one item, which would be unexpected for a counter_ref
            try:
                next(result) # Try to get another item
                # If this doesn't raise StopIteration, it means more than one doc was returned.
                active_logger_instance = logging.getLogger(__name__) # Get a logger if not available
                active_logger_instance.warning(f"WARNING: transaction_obj.get(counter_ref_arg) for {counter_ref_arg.path} returned more than one document in _get_next_id_tx. Using the first one.")
            except StopIteration:
                pass # Expected if only one document (or zero) was yielded
        except Exception as e_gen:
            active_logger_instance = logging.getLogger(__name__)
            active_logger_instance.error(f"Error iterating generator from transaction_obj.get: {e_gen}")
            counter_snapshot = None # Ensure it's None on error
    elif isinstance(result, firestore.DocumentSnapshot):
        counter_snapshot = result # It was a snapshot as expected
    else:
        # Log an unexpected type if it's neither a known snapshot nor an iterator
        active_logger_instance = logging.getLogger(__name__)
        active_logger_instance.warning(f"transaction_obj.get(counter_ref_arg) returned unexpected type: {type(result)}. Expected DocumentSnapshot or generator.")
        counter_snapshot = None

    current_sequence = 0
    # Now, counter_snapshot is either a DocumentSnapshot or None
    if counter_snapshot and counter_snapshot.exists: # Check if snapshot is not None AND exists
        current_sequence = counter_snapshot.to_dict().get("current_sequence", 0)
    
    next_sequence = current_sequence + 1
    transaction_obj.set(counter_ref_arg, {"current_sequence": next_sequence})
    return f"{project_abbreviation_arg}_{next_sequence}"

def generate_sequential_document_id(db, project_config, project_abbreviation, main_url_for_fallback, active_logger_instance):
    active_logger = active_logger_instance or logging.getLogger(__name__)

    # --- Start Debugging (keep this for now) ---
    active_logger.info(f"---generate_sequential_document_id DEBUG---")
    active_logger.info(f"Type of 'db' object: {type(db)}")
    active_logger.info(f"Is 'db' an instance of firestore.Client? {isinstance(db, firestore.Client)}")
    import google.cloud.firestore as gc_firestore # Alias to avoid conflict if firestore module is also local
    active_logger.info(f"Is 'db' an instance of google.cloud.firestore.Client? {isinstance(db, gc_firestore.Client)}")
    has_run_transaction_method = hasattr(db, 'run_transaction')
    active_logger.info(f"Does 'db' object have 'run_transaction' method? {has_run_transaction_method}")
    if not has_run_transaction_method: # Only log all attributes if the method is confirmed missing
        active_logger.warning(f"Attributes of 'db' object: {dir(db)}")
    # --- End Debugging ---

    counters_collection_name = project_config.get("firestore_counters_collection", "doc_counters")
    counter_doc_name = f"{project_abbreviation}_id_sequence"
    counter_ref = db.collection(counters_collection_name).document(counter_doc_name)

    new_document_id = None
    retries = 0
    delay = INITIAL_RETRY_DELAY_SECONDS

    # Define the transactional function using the decorator
    # The transaction object will be passed as the first argument to the decorated function
    @firestore.transactional 
    def _transactional_update_callable(transaction_obj, ref, abbr):
        # This decorated function will now call your original logic
        return _get_next_id_tx(transaction_obj, ref, abbr)

    while retries < MAX_TRANSACTION_RETRIES:
        transaction_reference_for_decorator = db.transaction() # Get a transaction reference for the decorator
        try:
            # Pass the transaction reference and other args to the decorated function
            new_document_id = _transactional_update_callable(
                transaction_reference_for_decorator, 
                counter_ref, 
                project_abbreviation
            )
            
            active_logger.info(f"Successfully generated sequential Document ID: {new_document_id} (attempt {retries + 1})")
            return new_document_id

        except (Aborted, DeadlineExceeded, ServiceUnavailable) as e:
            active_logger.warning(f"Transaction for sequential ID failed with {type(e).__name__} (attempt {retries + 1}/{MAX_TRANSACTION_RETRIES}): {e}. Retrying in {delay}s...")
            time.sleep(delay)
            retries += 1
            delay *= 2
        except Exception as e:
            active_logger.error(f"Unexpected error during transactional update for sequential ID: {e}", exc_info=True)
            break 

    # Fallback logic
    active_logger.error(f"All retries for sequential ID generation failed for {project_abbreviation}. Falling back to timestamp-based ID.")
    now = datetime.now(timezone.utc)
    timestamp_str = now.strftime("%Y%m%d_%H%M%S_%f")[:-3]
    url_hash_fallback = hashlib.md5(main_url_for_fallback.encode()).hexdigest()[:8]
    fallback_id = f"{project_abbreviation}_FALLBACK_{timestamp_str}_{url_hash_fallback}"
    active_logger.warning(f"Generated fallback Document ID: {fallback_id}")
    return fallback_id


# --- Helper Functions ---
def extract_title_from_url(url):
    """Extract a reasonable title from URL"""
    try:
        path = urlparse(url).path
        filename = os.path.basename(path)
        
        title = os.path.splitext(filename)[0]
        title = title.replace('-', ' ').replace('_', ' ')
        # Capitalize each word, more robustly
        title_parts = []
        for word in title.split():
            if word: # Ensure word is not empty
                title_parts.append(word[0].upper() + word[1:])
        title = ' '.join(title_parts)
        return title if title else "Document"
    except Exception:
        # Fallback if any error occurs during parsing
        return "Document"

def extract_date_from_metadata(metadata, active_logger_instance=None):
    """Extract date from metadata (description or table_row_data)."""
    active_logger = active_logger_instance or logging.getLogger(__name__)
    try:
        # Consolidate text sources for date extraction
        text_parts = []
        description = metadata.get("description")
        if description and isinstance(description, str):
            text_parts.append(description)
        
        table_row_data = metadata.get("table_row_data")
        if isinstance(table_row_data, list):
            text_parts.append(" ".join(str(item) for item in table_row_data if item)) # Join non-empty items
        elif isinstance(table_row_data, str): # If it's already a string
            text_parts.append(table_row_data)

        # Add other potential date-containing fields from metadata if they exist
        # For example, if "deliverables" or "publication_info" might be present
        # text_parts.append(str(metadata.get("deliverables", ""))) 

        text_to_search = " ".join(filter(None, text_parts))
        if not text_to_search.strip():
            active_logger.debug("No text content found in metadata for date extraction.")
            return None

        # More specific regex patterns, ordered by preference
        # YYYY-MM-DD (ISO format)
        iso_date_match = re.search(r'\b(20\d{2}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01]))\b', text_to_search)
        if iso_date_match:
            return iso_date_match.group(1)

        # DD Month YYYY (e.g., 28 June 2024, 1st May 2023)
        # Handles optional day ordinal (st, nd, rd, th)
        day_month_year_match = re.search(
            r'\b(0?[1-9]|[12]\d|3[01])(?:st|nd|rd|th)?\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(20\d{2})\b',
            text_to_search, re.IGNORECASE
        )
        if day_month_year_match:
            day, month_name, year = day_month_year_match.groups()
            month_map = {name.lower(): f"{i:02d}" for i, name in enumerate(["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"], 1)}
            return f"{year}-{month_map[month_name.lower()]}-{int(day):02d}"

        # Month YYYY (e.g., June 2024, Dec 2023) - default to 1st day
        month_year_match = re.search(
            r'\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(20\d{2})\b',
            text_to_search, re.IGNORECASE
        )
        if month_year_match:
            month_name, year = month_year_match.groups()
            month_map = {name.lower(): f"{i:02d}" for i, name in enumerate(["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"], 1)}
            return f"{year}-{month_map[month_name.lower()]}-01"
            
        # Quarter (e.g., Q1 2023, Quarter 2 2024)
        quarter_match = re.search(r'(?:Q([1-4])|Quarter\s+([1-4]))\s+(20\d{2})', text_to_search, re.IGNORECASE)
        if quarter_match:
            q_num_group1, q_num_group2, year = quarter_match.groups()
            quarter_num = q_num_group1 or q_num_group2
            quarter_end_dates = {'1': '03-31', '2': '06-30', '3': '09-30', '4': '12-31'}
            return f"{year}-{quarter_end_dates[quarter_num]}"
        
        # Half year (e.g., H1 2023, First Half 2024)
        half_match = re.search(r'(?:H([1-2])|(?:First|Second)\s+Half)\s+(20\d{2})', text_to_search, re.IGNORECASE)
        if half_match:
            h_num, year_val = half_match.groups()
            if h_num: # H1 or H2
                half_end_month_day = '06-30' if h_num == '1' else '12-31'
            elif "first half" in text_to_search.lower():
                half_end_month_day = '06-30'
            else: # Second half
                half_end_month_day = '12-31'
            return f"{year_val}-{half_end_month_day}"
        
        # Year only (e.g., 2023) - default to mid-year or specific configured default day/month
        year_only_match = re.search(r'\b(20\d{2})\b', text_to_search)
        if year_only_match:
            # Default to June 30th or as configured
            return f"{year_only_match.group(1)}-06-30" 
        
        active_logger.debug(f"No clear date pattern matched in metadata text: '{text_to_search[:200]}...'")
        return None
    except Exception as e:
        active_logger.warning(f"Error during date extraction from metadata: {e}", exc_info=True)
        return None

def extract_topics_from_content(title, description, table_context, page_context, project_config, active_logger_instance=None):
    active_logger = active_logger_instance or logging.getLogger(__name__)
    try:
        topic_config = project_config.get("topic_extraction", {})
        # Ensure categories from config are used if available
        topic_categories_config = topic_config.get("categories", {})
        # Fallback to generic patterns only if config categories are not defined or empty
        use_generic_fallback = not topic_categories_config 

        combined_text_elements = [
            str(title or "").lower(),
            str(description or "").lower(),
            str(table_context or "").lower(),
            str(page_context or "").lower()
        ]
        combined_text = " ".join(filter(None, combined_text_elements)).strip()

        if not combined_text:
            return [''] * 6 # Return 6 empty strings if no text

        extracted_topics = []
        topic_scores = {}

        if not use_generic_fallback:
            active_logger.debug(f"Using configured topic categories for extraction. Found {len(topic_categories_config)} categories.")
            for category_name, keywords in topic_categories_config.items():
                if not isinstance(keywords, list): 
                    active_logger.warning(f"Keywords for topic category '{category_name}' is not a list. Skipping.")
                    continue
                current_score = 0
                for keyword_phrase in keywords:
                    # Check for whole word/phrase matches to avoid partial hits (e.g., "fin" in "finance")
                    # Regex word boundaries \b might be useful here if keywords are single words
                    # For phrases, simple 'in' is often okay, but can be refined.
                    if keyword_phrase.lower() in combined_text:
                        current_score += len(keyword_phrase.split()) # Basic scoring by length of phrase
                        # Bonus for exact match of title/description fragments if meaningful
                        if keyword_phrase.lower() == str(title or "").lower().strip() or \
                           keyword_phrase.lower() == str(description or "").lower().strip():
                            current_score += 3 # Arbitrary bonus
                if current_score > 0:
                    topic_scores[category_name.replace('_', ' ')] = current_score # Store with spaces
            
            # Sort by score and take top N (e.g., up to 6)
            sorted_by_score = sorted(topic_scores.items(), key=lambda item: item[1], reverse=True)
            extracted_topics = [topic for topic, score in sorted_by_score[:6]]
        
        else: # Fallback to generic patterns
            active_logger.debug("Using generic topic patterns for extraction.")
            generic_patterns = project_config.get("generic_topic_patterns", {
                'Regulatory Compliance': ['regulation', 'regulatory', 'compliance', 'rule', 'policy', 'guideline'],
                'Financial Markets': ['financial market', 'market integrity', 'trading', 'exchange', 'securities'],
                'Consumer Protection': ['consumer protection', 'disclosure', 'investor rights', 'advice quality'],
                'Corporate Governance': ['corporate governance', 'director duties', 'company law'],
                'Enforcement Actions': ['enforcement', 'investigation', 'penalty', 'breach'],
                'Superannuation & Retirement': ['superannuation', 'smsf', 'retirement income'],
                'Licensing & Registration': ['licensing', 'afsl', 'registration', 'authorization'],
                'Digital Finance & Crypto': ['crypto-asset', 'digital finance', 'fintech', 'blockchain']
            })
            for topic_name, keywords in generic_patterns.items():
                if any(keyword.lower() in combined_text for keyword in keywords):
                    extracted_topics.append(topic_name)
                if len(extracted_topics) >= 6: break # Limit to 6

        # If still no topics, try to infer from primary contexts
        if not extracted_topics:
            active_logger.debug("No topics matched by patterns, attempting inference from context.")
            if table_context: extracted_topics.append(' '.join(str(table_context).split()[:3]).capitalize())
            elif page_context: extracted_topics.append(' '.join(str(page_context).split()[:3]).capitalize())
            elif title: extracted_topics.append(' '.join(str(title).split()[:3]).capitalize())
            else: extracted_topics.append('General Document') # Last resort

        # Ensure unique topics and pad to 6
        unique_topics = list(dict.fromkeys(filter(None, extracted_topics)))[:6]
        while len(unique_topics) < 6:
            unique_topics.append('')
            
        active_logger.info(f"Extracted topics: {unique_topics}")
        return unique_topics

    except Exception as e:
        active_logger.error(f"Error in topic extraction: {e}", exc_info=True)
        return [''] * 6


def extract_legislation_from_content(title, description, project_config, active_logger_instance=None):
    active_logger = active_logger_instance or logging.getLogger(__name__)
    try:
        legislation_config = project_config.get("legislation_extraction", {})
        # Explicit patterns for specific acts, allowing variations (e.g., with or without year)
        legislation_patterns_config = legislation_config.get("patterns", {
            # Example: "Corporations Act 2001": [r"\bCorporations Act\s*(?:2001)?\b", r"\bCorp Act\b"],
            #          "ASIC Act 2001": [r"\bASIC Act\s*(?:2001)?\b"],
            #          "National Consumer Credit Protection Act 2009": [r"\bNational Consumer Credit Protection Act\s*(?:2009)?\b", r"\bNCCP Act\b"]
        })
        default_acts_to_check = legislation_config.get("default_acts", [
            # "Corporations Act 2001", "ASIC Act 2001" # These would be keys in patterns_config
        ])

        combined_text_elements = [str(title or "").lower(), str(description or "").lower()]
        combined_text = " ".join(filter(None, combined_text_elements)).strip()

        if not combined_text:
            return []

        found_legislation_set = set()

        # Check configured patterns first
        for act_name, regex_patterns in legislation_patterns_config.items():
            if not isinstance(regex_patterns, list): continue
            for pattern_str in regex_patterns:
                try:
                    if re.search(pattern_str, combined_text, re.IGNORECASE):
                        found_legislation_set.add(act_name) # Add the canonical name
                        break # Found this act, move to the next act_name
                except re.error as re_e:
                    active_logger.warning(f"Regex error for legislation pattern '{pattern_str}' for act '{act_name}': {re_e}")
        
        # If fewer than 3 found, try more generic regex for "Any Act Name YYYY"
        if len(found_legislation_set) < 3:
            # This regex looks for multi-word capitalized phrases followed by "Act" and optionally a year.
            # It's broader and might need refinement to avoid false positives.
            generic_act_matches = re.findall(
                r"\b((?:[A-Z][a-z'-]+(?:\s+[A-Z][a-z'-]+)*\s+)*Act(?:\s+\d{4})?)\b", 
                str(title or "") + " " + str(description or "") # Search in original case for capitalization cues
            )
            for act_match_tuple in generic_act_matches:
                # re.findall with multiple groups can return tuples
                act_text = act_match_tuple[0] if isinstance(act_match_tuple, tuple) else act_match_tuple
                cleaned_act = ' '.join(act_text.split()) # Normalize spacing
                if cleaned_act and cleaned_act not in found_legislation_set and len(cleaned_act) > 7: # Basic filter
                    found_legislation_set.add(cleaned_act)
                    if len(found_legislation_set) >= 3: break
        
        # If still under limit, add defaults if they aren't already found (and are valid keys in patterns)
        for default_act_key in default_acts_to_check:
            if len(found_legislation_set) >= 3: break
            if default_act_key in legislation_patterns_config and default_act_key not in found_legislation_set:
                found_legislation_set.add(default_act_key)

        final_legislation_list = list(found_legislation_set)[:3]
        active_logger.info(f"Extracted legislation: {final_legislation_list}")
        return final_legislation_list

    except Exception as e:
        active_logger.error(f"Error in legislation extraction: {e}", exc_info=True)
        return []


def classify_document_type(title, url, document_type_hint, project_config, active_logger_instance=None):
    active_logger = active_logger_instance or logging.getLogger(__name__)
    try:
        doc_type_config = project_config.get("document_type_classification", {})
        # Configured patterns: dictionary where keys are doc types, values are lists of keywords/phrases
        type_patterns_config = doc_type_config.get("patterns", {})

        # Prioritize hint if it's specific and not generic HTML/Unknown
        if document_type_hint and document_type_hint not in ["HTML_DOCUMENT", "UNKNOWN_TYPE", "STRUCTURED_DATA"]:
            if document_type_hint == "PDF_DOCUMENT":
                active_logger.debug(f"Classified as 'PDF Document' based on hint.")
                return "PDF Document"

        text_to_classify = f"{str(title or '').lower()} {urlparse(str(url or '')).path.lower()}"
        if document_type_hint == "STRUCTURED_DATA": # Special case for structured data hint
             text_to_classify += " structured data table"

        if not text_to_classify.strip() and not (url and str(url).lower().endswith('.pdf')):
             active_logger.debug("No significant text for classification, defaulting to 'Document'.")
             return "Document"

        if type_patterns_config:
            doc_type_scores = {}
            for type_name, keywords in type_patterns_config.items():
                if not isinstance(keywords, list): continue
                score = 0
                for keyword in keywords:
                    kw_lower = keyword.lower()
                    if kw_lower in text_to_classify:
                        score += len(kw_lower.split())
                        if kw_lower in str(title or "").lower() or \
                           (len(kw_lower) < 6 and kw_lower.strip() in str(title or "")):
                            score += 2
                if score > 0:
                    doc_type_scores[type_name] = score
            
            if doc_type_scores:
                best_type = max(doc_type_scores, key=doc_type_scores.get)
                active_logger.info(f"Classified as '{best_type}' based on configured patterns. Scores: {doc_type_scores}")
                return best_type

        active_logger.debug("No configured pattern match or config empty, using generic ordered patterns.")
        generic_patterns_ordered = project_config.get("generic_document_type_patterns_ordered", [
            ('Media Release', ['media release', '/mr-', 'news/', '/statement/', 'announcement']),
            ('Regulatory Guide', ['regulatory guide', '/rg-', 'guidance document']),
            ('Consultation Paper', ['consultation paper', '/cp-', 'consultation draft']),
            ('Report', ['report', '/rep-', 'review', 'findings', 'survey', 'study', 'annual report']),
            ('Information Sheet', ['information sheet', 'info sheet', 'fact sheet', 'infosheet']),
            ('Legislative Instrument', ['legislative instrument', 'class order', 'instrument']), # CORRECTED LINE
            ('Form', ['form', '/form-', 'application form']),
            ('Speech', ['speech', 'keynote', 'address by']),
            ('Newsletter', ['newsletter', 'update bulletin', 'market integrity update']),
            ('Corporate Plan', ['corporate plan', 'strategic plan'])
        ])
        for doc_type_name, keywords in generic_patterns_ordered:
            if any(keyword.lower() in text_to_classify for keyword in keywords):
                active_logger.info(f"Classified as '{doc_type_name}' based on generic patterns.")
                return doc_type_name
        
        if url and str(url).lower().endswith('.pdf'):
            active_logger.debug("Classified as 'PDF Document' based on URL extension.")
            return "PDF Document"
        if document_type_hint == "STRUCTURED_DATA":
            active_logger.debug("Classified as 'Structured Data' based on hint.")
            return "Structured Data"

        active_logger.info("Defaulted to 'Document' after all classification attempts.")
        return "Document"

    except Exception as e:
        active_logger.error(f"Error in document type classification: {e}", exc_info=True)
        return "Document"


# --- Main Cloud Function ---
@functions_framework.cloud_event
def extract_initial_metadata(cloud_event):
    """
    Cloud Function to extract initial metadata, generate a unique Document_ID,
    and prepare data for further processing. Uses the Document_ID as Firestore document name.
    """
    active_logger = logging.getLogger(__name__) # Initial logger
    if not active_logger.hasHandlers(): # Ensure basicConfig is called only once
        log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
        logging.basicConfig(level=log_level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    publisher_client = None # Initialize to None
    gcp_project_id_env = os.environ.get("GCP_PROJECT")
    
    # Initialize message payload variables to ensure they exist in `except` blocks
    pubsub_message = {}
    customer = "unknown_customer" # Default value
    project_config_name = "unknown_project" # Default value
    main_url = "unknown_url" # Default value
    message_id = "N/A" # Default value

    try:
        # Decode Pub/Sub message
        if isinstance(cloud_event.data, dict) and "message" in cloud_event.data and "data" in cloud_event.data["message"]:
            pubsub_message_data_encoded = cloud_event.data["message"]["data"]
            message_id = cloud_event.data["message"].get("messageId", "N/A")
        else:
            active_logger.error(f"Unexpected cloud_event.data structure: {cloud_event.data}")
            return {"status": "error_bad_event_structure", "message": "Invalid CloudEvent data structure."}

        pubsub_message_decoded = base64.b64decode(pubsub_message_data_encoded).decode("utf-8")
        pubsub_message = json.loads(pubsub_message_decoded)

        # Extract primary identifiers from message
        customer = pubsub_message.get("customer")
        project_config_name = pubsub_message.get("project")
        main_url = pubsub_message.get("main_url")

        # Setup customer/project specific logging as early as possible
        # This will reconfigure the logger if customer/project are valid
        active_logger = setup_logging(customer, project_config_name) 
        active_logger.info(f"Processing Pub/Sub message_id: {message_id} for main_url: {main_url}")
        active_logger.debug(f"Received Pub/Sub payload: {pubsub_message}")

        # Validate required Pub/Sub message fields
        if not all([customer, project_config_name, main_url]):
            missing_fields = [field for field, value in [("customer",customer), ("project",project_config_name), ("main_url",main_url)] if not value]
            active_logger.error(f"Missing required Pub/Sub fields: {missing_fields}. Full message: {pubsub_message}")
            # Acknowledge to prevent retries for malformed messages
            return {"status": "error_bad_request", "message": f"Missing required Pub/Sub fields: {', '.join(missing_fields)}.", "message_id": message_id}

        # Load customer and project configurations
        customer_config = load_customer_config(customer)
        if not customer_config:
            active_logger.error(f"Failed to load customer_config for '{customer}'.")
            return {"status": "error_config", "message": f"Customer config for '{customer}' not found."}

        gcp_project_id = customer_config.get("gcp_project_id", gcp_project_id_env)
        if not gcp_project_id:
            active_logger.error("GCP Project ID not found in customer_config or environment variables.")
            return {"status": "error_config", "message": "GCP Project ID not configured."}

        db = firestore.Client(
            project=gcp_project_id,
            database=customer_config.get("firestore_database_id", "(default)") # Ensure (default) is used if not specified
        )
        project_config = load_dynamic_site_config(db, project_config_name, active_logger) # Pass active_logger
        if not project_config:
             active_logger.error(f"Failed to load project_config for '{project_config_name}'. Cannot proceed.")
             return {"status": "error_config", "message": f"Project config '{project_config_name}' not found."}

        publisher_client = pubsub_v1.PublisherClient()
        
        # --- Document ID Generation ---
        project_abbreviation = project_config.get("project_abbreviation", "DOC") # Default abbreviation
        final_document_id = generate_sequential_document_id(db, project_config, project_abbreviation, main_url, active_logger) # Pass active_logger
        
        url_hash = generate_url_hash(main_url) # Generate hash from the main URL

        # --- Prepare Base Firestore Data ---
        # Extract data from Pub/Sub message, providing defaults for robustness
        source_category_url = pubsub_message.get("source_category_url", pubsub_message.get("category_url", main_url)) # Fallback to main_url if category not present
        source_page_url = pubsub_message.get("source_page_url", source_category_url) # Fallback to source_category_url
        document_type_hint = pubsub_message.get("document_type", "HTML_DOCUMENT") # Default hint
        document_metadata_from_discovery = pubsub_message.get("document_metadata", {})
        category_page_metadata_from_discovery = pubsub_message.get("category_page_metadata", {})

        # Build the initial set of data for Firestore
        final_firestore_data = {
            "main_url": main_url,
            "url_hash_identifier": url_hash,
            "indexed_filename_base": final_document_id, # Used for XML/GCS naming
            "Document_ID": final_document_id, # The primary identifier for this document
            "source_category_url": source_category_url,
            "source_page_url": source_page_url,
            "document_type_hint_from_discovery": document_type_hint,
            "processing_status": "metadata_extraction_started", # Initial status
            "scrape_timestamp": firestore.SERVER_TIMESTAMP, # When this metadata extraction started
            "last_updated": firestore.SERVER_TIMESTAMP, # Will be updated as processing continues
            "pipeline_version": project_config.get("pipeline_version_tag", "1.0.0"), # Track version
             # Spread discovered metadata, ensuring no sensitive fields are directly copied if not intended
            **{sanitize_field_name(k): v for k, v in document_metadata_from_discovery.items()},
            **{sanitize_field_name(k): v for k, v in category_page_metadata_from_discovery.items()}
        }
        active_logger.debug(f"Initial Firestore data prepared for {final_document_id}: {final_firestore_data}")


        # --- Metadata Enrichment (Title, Date, Type, Topics, Legislation) ---
        # Use title from discovery if available, else extract from URL
        temp_title = final_firestore_data.get("title") or \
                     document_metadata_from_discovery.get("title") or \
                     extract_title_from_url(main_url)
        
        temp_description = final_firestore_data.get("description", "") or \
                           document_metadata_from_discovery.get("description", "")
        
        # Contexts from discovery step if available
        temp_table_ctx = final_firestore_data.get("table_context", "") or \
                         document_metadata_from_discovery.get("table_context", "")
        temp_page_ctx = final_firestore_data.get("page_context", "") or \
                        document_metadata_from_discovery.get("page_context", "")


        # Classify document type using hints and content
        classified_doc_type = classify_document_type(temp_title, main_url, document_type_hint, project_config, active_logger) # Pass active_logger
        
        # Extract release date - prioritize explicit "Release_Date" from discovery, then rule-based, then default
        extracted_release_date_str = final_firestore_data.get("Release_Date") or \
                                     document_metadata_from_discovery.get("Release_Date") or \
                                     extract_date_from_metadata(document_metadata_from_discovery, active_logger) # Pass active_logger
        
        if not extracted_release_date_str or not re.match(r"^\d{4}-\d{2}-\d{2}$", extracted_release_date_str): # Validate format or if empty
            active_logger.debug(f"Release date '{extracted_release_date_str}' invalid or not found, defaulting.")
            extracted_release_date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d') # Default to current date
        
        # Populate standard metadata fields
        final_firestore_data["Document_Title"] = temp_title
        final_firestore_data["Document_Type"] = classified_doc_type
        final_firestore_data["Release_Date"] = extracted_release_date_str 
        final_firestore_data["Year"] = extracted_release_date_str.split('-')[0] # Extract year from the (potentially new) release date
        
        # Extract topics and legislation using enriched context
        final_firestore_data["Topics"] = extract_topics_from_content(
            temp_title, temp_description, temp_table_ctx, temp_page_ctx, project_config, active_logger # Pass active_logger
        )
        final_firestore_data["Key_Legislation_Mentioned"] = extract_legislation_from_content(
            temp_title, temp_description, project_config, active_logger # Pass active_logger
        )
        final_firestore_data["Summary_Description"] = temp_description # Use the consolidated description


        # --- Handle Different Document Types (PDF, Structured Data, HTML) ---
        firestore_collection_name = project_config.get("firestore_collection")
        if not firestore_collection_name:
            active_logger.error("'firestore_collection' not defined in project_config.")
            # Critical config error, acknowledge to prevent retries on bad config
            return {"status": "error_config", "message": "'firestore_collection' not defined in project config."}
        
        doc_ref = db.collection(firestore_collection_name).document(final_document_id) # Use the generated ID for the document

        # Logic for PDF documents
        if document_type_hint == "PDF_DOCUMENT" or (classified_doc_type == "PDF Document" and main_url.lower().endswith('.pdf')):
            final_firestore_data.update({
                "processing_status": "pdf_metadata_finalized", # PDF metadata is considered final here
                "requires_html_scraping": False, # No HTML scraping needed for a PDF link
                "ai_metadata_extraction_successful": False, # No AI extraction run at this stage for PDFs
            })
            doc_ref.set(final_firestore_data, merge=True) # Save to Firestore
            active_logger.info(f"Finalized metadata for PDF: {final_document_id}. Title: '{temp_title}'")
            
            # Publish to fetch_content topic for PDF download
            pub_payload_fetch_content = {
                "customer": customer, "project": project_config_name,
                "identifier": final_document_id, "main_url": main_url # Pass the PDF URL
            }
            output_topic_path = publisher_client.topic_path(gcp_project_id, NEXT_STEP_FETCH_CONTENT_TOPIC_NAME)
            future = publisher_client.publish(output_topic_path, json.dumps(pub_payload_fetch_content).encode('utf-8'))
            future.result(timeout=30) # Wait for publish confirmation with timeout
            active_logger.info(f"Published PDF {final_document_id} to {NEXT_STEP_FETCH_CONTENT_TOPIC_NAME} for download.")
            return {"status": "success_pdf", "identifier": final_document_id, "document_type_classified": classified_doc_type}
        
        # Logic for Structured Data (e.g. a row from a table that is the document itself)
        elif document_type_hint == "STRUCTURED_DATA":
            final_firestore_data.update({
                "processing_status": "structured_data_finalized",
                "requires_html_scraping": False, # Structured data itself is the content
                "ai_metadata_extraction_successful": False, # Typically no AI on the raw structured data at this point
                # If structured data implies no separate HTML content:
                "gcs_html_path": None, 
                "html_content_available": False
            })
            doc_ref.set(final_firestore_data, merge=True)
            active_logger.info(f"Finalized metadata for Structured Data: {final_document_id}. Title: '{temp_title}'")
            
            # Publish directly to generate_xml topic if no further content fetching is needed
            xml_topic_path = publisher_client.topic_path(gcp_project_id, NEXT_STEP_GENERATE_XML_TOPIC_NAME)
            xml_payload = {
                "customer": customer, "project": project_config_name,
                "identifier": final_document_id, 
                "date": datetime.now(timezone.utc).strftime('%Y%m%d') # Current date for XML generation context
            }
            future = publisher_client.publish(xml_topic_path, json.dumps(xml_payload).encode('utf-8'))
            future.result(timeout=30)
            active_logger.info(f"Published Structured Data item {final_document_id} to {NEXT_STEP_GENERATE_XML_TOPIC_NAME}.")
            return {"status": "success_structured_data", "identifier": final_document_id, "document_type_classified": classified_doc_type}
        
        # Logic for HTML documents (or others requiring further processing/AI)
        else:
            final_firestore_data["requires_html_scraping"] = True # Assume HTML needs to be fetched by next step
            html_content_for_ai_metadata = None # Initialize
            
            # --- AI Metadata Extraction (if enabled and applicable) ---
            ai_extraction_config = project_config.get("ai_metadata_extraction", {})
            # Check if AI extraction is enabled globally and for this document type (optional refinement)
            ai_enabled_for_project = ai_extraction_config.get("enabled", False) # Default to False if not specified
            
            if ai_enabled_for_project:
                active_logger.info(f"AI metadata extraction enabled. Attempting to fetch HTML for AI analysis of {main_url}")
                try:
                    scrape_timeout = project_config.get("metadata_scrape_timeout_seconds", 20) # Shorter timeout for this specific fetch
                    user_agent_metadata = project_config.get("metadata_scraper_user_agent", f"GoogleCloudFunctions-ExtractInitialMetadata-AI/{final_firestore_data['pipeline_version']}")
                    
                    response = requests.get(main_url, timeout=scrape_timeout, headers={'User-Agent': user_agent_metadata})
                    response.raise_for_status() # Check for HTTP errors
                    
                    html_content_for_ai_metadata = validate_html_content(response.content, active_logger) # Validate
                    if not html_content_for_ai_metadata:
                         active_logger.warning(f"HTML content validation failed or empty for AI metadata from {main_url}.")
                         final_firestore_data["error_message"] = final_firestore_data.get("error_message","") + "HTML content invalid/empty for AI metadata. "
                         final_firestore_data["processing_status"] = "html_content_invalid_for_ai_metadata"
                
                except requests.exceptions.RequestException as e_req:
                    scrape_error_message = f"Failed to scrape MainURL {main_url} for AI metadata: {str(e_req)}"
                    active_logger.error(scrape_error_message)
                    final_firestore_data["error_message"] = final_firestore_data.get("error_message","") + scrape_error_message
                    final_firestore_data["processing_status"] = "error_scraping_html_for_ai_metadata"
                except Exception as e_gen_scrape: # Catch any other scraping related errors
                    active_logger.error(f"Unexpected error during HTML fetch for AI for {main_url}: {e_gen_scrape}", exc_info=True)
                    final_firestore_data["error_message"] = final_firestore_data.get("error_message","") + f"Unexpected HTML fetch error: {str(e_gen_scrape)}"
                    final_firestore_data["processing_status"] = "error_unexpected_html_fetch_for_ai"

            
            if html_content_for_ai_metadata and ai_enabled_for_project: # Proceed with AI extraction if HTML was fetched and AI is on
                # Default AI extraction fields if not in project_config
                default_ai_fields = [
                    {"name": "Document_Title", "description": "The primary, official title of the document. Should be specific, complete, and informative."},
                    {"name": "Release_Date", "description": "The exact publication, release, or effective date (format YYYY-MM-DD). If only year, use YYYY-01-01. Prioritize the most prominent date."},
                    {"name": "Document_Type", "description": "The specific type (e.g., Report, Regulatory Guide, Media Release, Consultation Paper, Form, Speech, Alert, Update)."},
                    {"name": "Topics", "description": "A JSON array of 3-6 relevant topics or keywords (e.g., [\"Market Integrity\", \"Consumer Credit\", \"Licensing\"]). Return empty array [] if none."},
                    {"name": "Summary_Description", "description": "A concise 1-3 sentence summary of the document's main purpose, key findings, or impact. Avoid boilerplate."},
                    {"name": "Key_Legislation_Mentioned", "description": "A JSON array of official names of key Acts, Regulations, or Standards mentioned (e.g., [\"Corporations Act 2001\", \"ASIC Act 2001\"]). Return empty array [] if none."}
                ]
                ai_extraction_fields = ai_extraction_config.get("fields", default_ai_fields)
                ai_extraction_schema_names = [field["name"] for field in ai_extraction_fields]

                try:
                    # Initialize Vertex AI (consider doing this once outside the function if performance is critical and using instances)
                    vertex_ai_location = project_config.get("vertex_ai_location", "us-central1")
                    vertexai.init(project=gcp_project_id, location=vertex_ai_location)
                    
                    ai_model_params = project_config.get("ai_model_config", {})
                    model_id = ai_model_params.get("metadata_extraction_model_id", "gemini-1.5-flash-001") # Ensure this model is available
                    
                    llm_model = GenerativeModel(model_id)
                    
                    # Truncate HTML to avoid exceeding model token limits (MAX_HTML_FOR_PROMPT)
                    truncated_html = html_content_for_ai_metadata[:MAX_HTML_FOR_PROMPT]
                    if len(html_content_for_ai_metadata) > MAX_HTML_FOR_PROMPT:
                        active_logger.warning(f"HTML for {main_url} was truncated from {len(html_content_for_ai_metadata)} to {MAX_HTML_FOR_PROMPT} chars for AI metadata extraction.")

                    prompt_fields_json_description = json.dumps(ai_extraction_fields, indent=2)
                    language_details = project_config.get("language_and_country", {}) # E.g. {"language_code": "en", "language": "English"}
                    language_prompt_hint = f"The document is primarily in {language_details.get('language','English')} ({language_details.get('language_code','en')})." if language_details.get('language_code') else "The document is primarily in English."
                    
                    # Construct a more robust prompt
                    prompt = f"""You are an expert in analyzing regulatory documents.
                    Analyze the following HTML content from the URL: {main_url}
                    Your task is to extract the specified metadata fields accurately.
                    {language_prompt_hint}

                    Return a single, valid JSON object that strictly adheres to this schema:
                    ```json
                    {prompt_fields_json_description}
                    ```

                    HTML Content (potentially truncated):
                    ```html
                    {truncated_html}
                    ```

                    Extraction Guidelines:
                    1.  **Accuracy is paramount.** Only extract information explicitly present or strongly implied by the content.
                    2.  **JSON Format:** Ensure your output is a single, valid JSON object.
                    3.  **Field Completeness:** Include all fields from the schema. If a value cannot be determined, use "Not Available" for strings, or an empty array `[]` for list types (like Topics, Key_Legislation_Mentioned). Do NOT omit fields.
                    4.  **Release_Date:** Format strictly as `YYYY-MM-DD`. If only a year is found, use the first day of that year (e.g., `YYYY-01-01`). If a month and year, use the first day of that month. Prioritize clear publication dates.
                    5.  **Error Pages:** If the content suggests an error page (404, Access Denied, etc.), set `Document_Title` to "Error Page Detected", `Summary_Description` to describe the error, and other fields appropriately (e.g., "Not Applicable" or empty arrays). Include a field `"error_page_detected": true` in your JSON if you detect this.
                    6.  **Content Focus:** Extract metadata from the main document content. Avoid boilerplate text (headers, footers, navigation menus) unless it's part of the document's core metadata.
                    7.  **Summaries:** Summary_Description should be neutral, factual, and concise (1-3 sentences).
                    8.  **Contextual Understanding**: If title or description is ambiguous, use surrounding HTML content for better interpretation.
                    """
                    
                    # Configure generation parameters from project_config or use defaults
                    generation_config_params = ai_model_params.get("metadata_extraction_generation_config", {
                        "temperature": 0.1, # Lower for more factual, less creative
                        "top_p": 0.8, 
                        "max_output_tokens": 2048, # Ensure sufficient for JSON
                    })
                    generation_config_params["response_mime_type"] = "application/json" # Request JSON output
                    
                    generation_config_llm = GenerationConfig(**generation_config_params)
                                        
                    active_logger.info(f"Requesting AI metadata extraction from model {model_id} for {main_url} (Timeout: {ai_model_params.get('timeout', 60)}s)")
                    
                    request_options = {"timeout": ai_model_params.get('timeout', 60)} # Timeout for the generate_content call
                    response = llm_model.generate_content(
                        [Part.from_text(prompt)],
                        generation_config=generation_config_llm,
                        request_options=request_options
                    )
                    
                    extracted_metadata_ai = {} # Initialize
                    if response.candidates and response.candidates[0].content.parts:
                        candidate_text = response.candidates[0].content.parts[0].text.strip()
                        active_logger.debug(f"AI raw JSON response for {main_url}: {candidate_text[:1000]}...") # Log more for debugging
                        try:
                            extracted_metadata_ai = json.loads(candidate_text)
                            if not isinstance(extracted_metadata_ai, dict):
                                active_logger.error(f"AI response for {main_url} was not a JSON object as expected: Type {type(extracted_metadata_ai)}. Content: {candidate_text[:200]}")
                                extracted_metadata_ai = {name: "AI_INVALID_JSON_RESPONSE_TYPE" for name in ai_extraction_schema_names} # Populate with error
                                final_firestore_data["processing_status"] = "ai_metadata_invalid_json_type"
                            
                            elif extracted_metadata_ai.get("error_page_detected") is True: # Explicitly check for boolean true
                                 active_logger.warning(f"AI detected an error page for {main_url}. Response: {extracted_metadata_ai}")
                                 final_firestore_data["processing_status"] = "ai_detected_error_page"
                                 final_firestore_data["Document_Title"] = extracted_metadata_ai.get("Document_Title", "Error Page Detected by AI")
                                 final_firestore_data["Summary_Description"] = extracted_metadata_ai.get("Summary_Description", "AI indicated this page is an error or non-document page.")
                                 # Nullify other fields or set to "Not Applicable" for error pages
                                 for schema_key in ai_extraction_schema_names:
                                     if schema_key not in extracted_metadata_ai: # If AI omitted some fields for error page
                                         if any(isinstance(final_firestore_data.get(schema_key), t) for t in [list, dict]):
                                             final_firestore_data[schema_key] = [] if isinstance(final_firestore_data.get(schema_key), list) else {}
                                         else:
                                             final_firestore_data[schema_key] = "Not Applicable - Error Page"
                        
                        except json.JSONDecodeError as e_json:
                            active_logger.error(f"Failed to decode AI JSON response for {main_url}: {e_json}. Response snippet: {candidate_text[:500]}")
                            extracted_metadata_ai = {name: "AI_JSON_PARSE_FAILED" for name in ai_extraction_schema_names} # Populate with error
                            final_firestore_data["processing_status"] = "ai_metadata_parse_failed"
                    else:
                        active_logger.warning(f"No valid candidates or parts in AI response for {main_url}. Full response: {response}")
                        extracted_metadata_ai = {name: "AI_NO_VALID_CANDIDATE" for name in ai_extraction_schema_names} # Populate with error
                        final_firestore_data["processing_status"] = "ai_metadata_no_candidate"

                    # --- Merge AI results with rule-based results ---
                    # Prioritize AI results for fields it was asked to extract, unless AI indicates an error or "Not Available" when a better rule-based value exists.
                    if final_firestore_data["processing_status"] not in ["ai_detected_error_page", "ai_metadata_parse_failed", "ai_metadata_invalid_json_type"]:
                        for key, ai_value in extracted_metadata_ai.items():
                            if key in ai_extraction_schema_names: # Only process keys AI was asked for
                                # Sanitize field name for Firestore if needed (though schema names should be safe)
                                firestore_key = sanitize_field_name(key)

                                # Handle "Not Available" or empty strings from AI for non-list fields
                                if isinstance(ai_value, str) and ai_value.strip().lower() in ["not available", "n/a", ""]:
                                    # If rule-based already has a good value, keep it. Otherwise, store "Not Available".
                                    if not final_firestore_data.get(firestore_key) or final_firestore_data.get(firestore_key) in ["Not Available", ""]:
                                        final_firestore_data[firestore_key] = "Not Available"
                                    # else: keep existing better rule-based value
                                
                                # Handle list types from AI (Topics, Key_Legislation_Mentioned)
                                elif isinstance(ai_value, list):
                                    # Ensure all items in the list are strings and filter out empties
                                    final_firestore_data[firestore_key] = [str(item).strip() for item in ai_value if str(item).strip()]
                                
                                elif ai_value is not None: # For other valid AI values (non-empty strings, numbers if applicable)
                                    final_firestore_data[firestore_key] = ai_value
                                # else: ai_value is None, do not overwrite existing data with None unless intended.

                        # Specifically re-evaluate Release_Date and Year from AI's output
                        ai_release_date = extracted_metadata_ai.get("Release_Date")
                        if isinstance(ai_release_date, str) and ai_release_date not in ["Not Available", ""] and re.match(r"^\d{4}-\d{2}-\d{2}$", ai_release_date):
                            final_firestore_data["Release_Date"] = ai_release_date
                            final_firestore_data["Year"] = ai_release_date.split('-')[0]
                        elif final_firestore_data.get("Release_Date", "") in ["Not Available", ""]: # if AI didn't provide good date, ensure rule-based or default is set
                            final_firestore_data["Release_Date"] = extracted_release_date_str # from earlier rule-based/default
                            final_firestore_data["Year"] = final_firestore_data["Release_Date"].split('-')[0]


                    # Update AI success status based on whether any error flags were set by AI part
                    is_ai_extraction_issue = any(status_flag in final_firestore_data["processing_status"] for status_flag in ["ai_invalid", "ai_parse_failed", "ai_no_candidate", "ai_detected_error_page"]) or \
                                            any(isinstance(val, str) and val.startswith("AI_") for val in extracted_metadata_ai.values())
                    
                    final_firestore_data["ai_metadata_extraction_successful"] = not is_ai_extraction_issue
                    if not is_ai_extraction_issue and final_firestore_data["processing_status"].startswith("metadata_extraction_started"): # If no prior error
                        final_firestore_data["processing_status"] = "metadata_extracted_with_ai"
                    elif is_ai_extraction_issue and not any(final_firestore_data["processing_status"].startswith(err_pref) for err_pref in ["error_", "ai_"]):
                        # If an AI issue occurred but status wasn't an AI-specific error yet
                        final_firestore_data["processing_status"] = "metadata_extraction_ai_issues"


                except vertexai.generative_models.generation_types.BlockedPromptException as bpe:
                    active_logger.error(f"AI prompt blocked for {main_url}. Reason: {bpe}", exc_info=True)
                    final_firestore_data["ai_metadata_extraction_successful"] = False
                    final_firestore_data["processing_status"] = "ai_prompt_blocked"
                    final_firestore_data["error_message"] = final_firestore_data.get("error_message","") + f"AI prompt blocked: {str(bpe)[:200]}"
                except Exception as e_llm: # Catch other LLM exceptions (timeouts, API errors)
                    active_logger.error(f"LLM metadata extraction call failed for {main_url}: {str(e_llm)}", exc_info=True)
                    final_firestore_data["ai_metadata_extraction_successful"] = False
                    final_firestore_data["processing_status"] = "ai_metadata_extraction_exception"
                    final_firestore_data["error_message"] = final_firestore_data.get("error_message","") + f"AI extraction exception: {str(e_llm)[:200]}"
            
            elif not ai_enabled_for_project: # AI explicitly disabled in config
                active_logger.info(f"AI metadata extraction is disabled for project '{project_config_name}'. Using rule-based metadata only.")
                final_firestore_data["ai_metadata_extraction_successful"] = False # Mark as false as AI didn't run
                if final_firestore_data["processing_status"].startswith("metadata_extraction_started"): # If not already an error
                    final_firestore_data["processing_status"] = "metadata_extracted_rules_only_ai_disabled"
            # else: AI was enabled, but HTML fetch failed - status and error_message already set

            # Ensure crucial date fields are always populated even if AI fails or is skipped
            if not final_firestore_data.get("Release_Date") or \
               not (isinstance(final_firestore_data.get("Release_Date"), str) and \
                    re.match(r"^\d{4}-\d{2}-\d{2}$", final_firestore_data.get("Release_Date"))):
                active_logger.debug(f"Final check: Release_Date '{final_firestore_data.get('Release_Date')}' invalid or missing. Setting from earlier default: {extracted_release_date_str}")
                final_firestore_data["Release_Date"] = extracted_release_date_str # Fallback to rule-based or current date
                final_firestore_data["Year"] = extracted_release_date_str.split('-')[0]


            # Final save to Firestore for HTML documents (and others that reach this branch)
            doc_ref.set(final_firestore_data, merge=True)
            active_logger.info(f"Stored/Updated metadata for item: {final_document_id}. Title: '{final_firestore_data.get('Document_Title')}'. Status: {final_firestore_data.get('processing_status')}")
            
            # Always trigger fetch_content for HTML documents (and others that are not PDF/StructuredData initially)
            # fetch-content will decide if full scraping is needed based on status and existing content.
            pub_payload_fetch_content = {
                "customer": customer, "project": project_config_name,
                "identifier": final_document_id, "main_url": main_url
            }
            output_topic_path = publisher_client.topic_path(gcp_project_id, NEXT_STEP_FETCH_CONTENT_TOPIC_NAME)
            future = publisher_client.publish(output_topic_path, json.dumps(pub_payload_fetch_content).encode('utf-8'))
            future.result(timeout=30) # Wait for publish confirmation
            active_logger.info(f"Published item {final_document_id} to {NEXT_STEP_FETCH_CONTENT_TOPIC_NAME} for content fetching.")
            return {"status": "success_html_type", "identifier": final_document_id, "document_type_classified": final_firestore_data.get("Document_Type", "HTML Document")}

    except ValueError as ve: # Catch configuration or input validation errors specifically
        # Log using the logger that was set up (or default if setup failed)
        active_logger.error(f"ValueError in extract_initial_metadata for URL '{main_url}': {str(ve)}", exc_info=True)
        # Acknowledge to Pub/Sub to prevent retries for fundamentally flawed messages
        return {"status": "error_value", "message": str(ve), "message_id": message_id}
    except Exception as e:
        # Use a logger that's guaranteed to be initialized (active_logger should be by this point)
        active_logger.error(f"Critical error in extract_initial_metadata for URL '{main_url}': {str(e)}", exc_info=True)
        # For critical, unexpected errors, re-raise to allow Pub/Sub to retry if configured,
        # or to be caught by global error handlers / dead-lettering.
        # If you want to prevent all retries at this stage, return a dict.
        # Depending on your dead-lettering strategy, re-raising might be appropriate.
        # For now, returning error to acknowledge and prevent poison pills if the error is persistent.
        return {
            "status": "error_critical_exception", 
            "message": f"An unexpected error occurred: {str(e)}", 
            "message_id": message_id,
            "main_url": main_url 
        }