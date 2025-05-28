# src/functions/extract_initial_metadata/main.py

import json
import base64
import logging
import os
import time
from datetime import datetime, timezone

import requests # For HTTP requests to scrape
import functions_framework
from google.cloud import firestore, pubsub_v1

import vertexai
from vertexai.generative_models import GenerativeModel, Part, GenerationConfig

from src.common.utils import generate_url_hash, setup_logging
# Updated import to include load_dynamic_site_config
from src.common.config import load_customer_config, load_dynamic_site_config
from src.common.helpers import generate_law_id, get_mapped_field, validate_html_content, sanitize_field_name

# --- Global Configuration ---
logger = logging.getLogger(__name__)
NEXT_STEP_FETCH_CONTENT_TOPIC_NAME = "fetch-content-topic"
MAX_HTML_FOR_PROMPT = 3000000  # Max characters of HTML to send to LLM
MAX_FIRESTORE_RETRIES = 3
RETRY_BACKOFF = 2

# --- Firestore Transactional Update for Sequence ---
@firestore.transactional
def update_sequence_number_transactional(transaction, sequence_doc_ref, new_sequence_value):
    """
    Updates the sequence number in Firestore within a transaction.
    Checks if the current value is less than the new value to prevent rollbacks.
    """
    sequence_snapshot = sequence_doc_ref.get(transaction=transaction)
    current_db_sequence = 0
    if sequence_snapshot.exists:
        current_db_sequence = sequence_snapshot.to_dict().get("last_sequence", 0)

    if new_sequence_value > current_db_sequence:
        transaction.set(sequence_doc_ref, {"last_sequence": new_sequence_value}, merge=True)
        return new_sequence_value
    return current_db_sequence

# --- Main Cloud Function ---
@functions_framework.cloud_event
def extract_initial_metadata(cloud_event):
    """
    Cloud Function to scrape a MainURL, extract metadata using AI,
    and store it in Firestore.
    """
    active_logger = logger # Base logger
    publisher_client = None
    gcp_project_id = os.environ.get("GCP_PROJECT") # Fallback GCP ID

    # Initialize pubsub_message to an empty dict for safer error logging
    pubsub_message = {}

    try:
        pubsub_message_data_encoded = cloud_event.data["message"]["data"]
        pubsub_message_decoded = base64.b64decode(pubsub_message_data_encoded).decode("utf-8")
        pubsub_message = json.loads(pubsub_message_decoded)

        message_id = cloud_event.data["message"].get("messageId", "N/A")
        publish_time = cloud_event.data["message"].get("publishTime", "N/A")

        customer = pubsub_message.get("customer")
        project_config_name = pubsub_message.get("project")
        main_url = pubsub_message.get("main_url")
        category_url = pubsub_message.get("category_url")
        category_page_metadata = pubsub_message.get("category_page_metadata", {})

        if not all([customer, project_config_name, main_url]):
            logger.error(f"Missing required Pub/Sub fields: customer, project, or main_url. MessageID: {message_id}")
            # Return a well-formed error response for bad requests, preventing Pub/Sub retries for this.
            return {"status": "error", "message": "Missing required Pub/Sub fields in message.", "message_id": message_id}

        # --- Configuration Loading ---
        customer_config = load_customer_config(customer)
        gcp_project_id_customer = customer_config.get("gcp_project_id")
        if gcp_project_id_customer:
            gcp_project_id = gcp_project_id_customer
        
        if not gcp_project_id:
            logger.error("GCP Project ID not found in customer config or environment.")
            return {"status": "error", "message": "GCP Project ID not configured."}

        active_logger = setup_logging(customer, project_config_name)
        active_logger.info(f"Received messageId: {message_id}, publishTime: {publish_time}. Processing MainURL: {main_url}")

        # --- Client Initialization ---
        db = firestore.Client(
            project=gcp_project_id,
            # project_config is loaded after db init now, so get firestore_database_id from customer_config or set a default
            # This assumes firestore_database_id might be in customer_config or a global default
            database=customer_config.get("firestore_database_id", "(default)")
        )
        
        # Load dynamic project configuration using the new function
        project_config = load_dynamic_site_config(db, project_config_name, active_logger)
        if not project_config: # load_dynamic_site_config should always return a config or raise error
             active_logger.error(f"Failed to load project configuration for {project_config_name}.")
             return {"status": "error", "message": f"Failed to load project configuration for {project_config_name}."}


        publisher_client = pubsub_v1.PublisherClient()
        
        identifier = generate_url_hash(main_url)
        firestore_collection_name = project_config.get("firestore_collection", "processedUrls")

        # --- 1. Scrape MainURL ---
        html_content_main_url = None
        try:
            active_logger.info(f"Scraping MainURL: {main_url}")
            # Timeout from project_config or a default
            scrape_timeout = project_config.get("metadata_scrape_timeout", 45)
            response = requests.get(main_url, timeout=scrape_timeout, headers={'User-Agent': 'GoogleCloudFunctions-ExtractInitialMetadata/1.0'})
            response.raise_for_status()
            html_content_main_url = validate_html_content(response.content, active_logger)
        except requests.exceptions.RequestException as e:
            active_logger.error(f"Failed to scrape MainURL {main_url}: {str(e)}")
            raise # Let Pub/Sub retry for transient network issues or retry_pipeline handle it
        
        if not html_content_main_url:
            active_logger.warning(f"No valid HTML content retrieved from {main_url}. Aborting.")
            doc_ref_error = db.collection(firestore_collection_name).document(identifier)
            doc_ref_error.set({
                "main_url": main_url, "category_url": category_url,
                "processing_status": "error_scraping_main_url",
                "error_message": "No valid HTML content",
                "last_updated": firestore.SERVER_TIMESTAMP
            }, merge=True)
            return {"status": "success_processed_error", "main_url": main_url, "detail": "No valid HTML"}

        # --- 2. AI Metadata Extraction ---
        extracted_metadata_ai = {}
        # Use the new config key 'metadata_extraction_fields_config'
        # This key is expected to be populated by load_dynamic_site_config
        # Default to a basic list if not found in config for robustness
        fields_to_extract_config = project_config.get("metadata_extraction_fields_config", ["title", "publication_date", "author"])
        
        # Ensure 'Law-ID' is not requested from AI if it's computed later
        # fields_to_extract_config could be a list of strings or list of dicts. Assuming list of strings for now.
        ai_extraction_schema = [field for field in fields_to_extract_config if isinstance(field, str) and field.lower() != "law-id"]

        if ai_extraction_schema:
            try:
                active_logger.info(f"Attempting AI metadata extraction for fields: {', '.join(ai_extraction_schema)}")
                
                vertex_ai_location = project_config.get("vertex_ai_location", "europe-west1") # Default location from config
                vertexai.init(project=gcp_project_id, location=vertex_ai_location)
                
                ai_model_config = project_config.get("ai_model_config", {})
                model_id = ai_model_config.get("metadata_extraction_model_id", "gemini-2.0-flash-001") # Model from config
                
                llm_model = GenerativeModel(model_id)
                
                truncated_html = html_content_main_url[:MAX_HTML_FOR_PROMPT]
                if len(html_content_main_url) > MAX_HTML_FOR_PROMPT:
                    active_logger.warning(f"HTML content for {main_url} was truncated to {MAX_HTML_FOR_PROMPT} chars for LLM prompt.")

                prompt_fields_str = ", ".join(f'"{field}"' for field in ai_extraction_schema)
                # Dynamic language for prompt (e.g. date format hints) could be added here if 'language_and_country' from dynamic config is used
                language_details = project_config.get("language_and_country", {})
                language_hint = f" (target language context: {language_details.get('language_code', 'en')})" if language_details else ""

                prompt = f"""Analyze the following HTML content from the URL {main_url}:
                ```html
                {truncated_html}
                ```
                Your task is to extract specific pieces of information{language_hint}.
                Please extract the following fields: [{prompt_fields_str}].
                Return the information STRICTLY as a JSON object where keys are the field names (e.g., "title", "publication_date") and values are the extracted text.
                If a field is not found or not applicable, its value should be "Not Available".
                For date fields (like 'publication_date'), if found, format them as YYYY-MM-DD. If the year is ambiguous, use the most recent plausible year.
                If the HTML is clearly not an article or document (e.g., an error page, a login page), return an empty JSON object or {{"error_page_detected": True}}.

                Example JSON output: {{ "title": "Example Title", "publication_date": "2023-01-15", "author": "John Doe" }}
                """
                
                generation_config_llm = GenerationConfig(
                    temperature=ai_model_config.get("temperature", 0.1),
                    top_p=ai_model_config.get("top_p", 0.9),
                    response_mime_type="application/json" # Request JSON output directly
                )

                response = llm_model.generate_content(
                    [Part.from_text(prompt)],
                    generation_config=generation_config_llm
                )
                
                if response.candidates and response.candidates[0].content.parts:
                    candidate_text = response.candidates[0].content.parts[0].text.strip()
                    try:
                        ai_response_json = json.loads(candidate_text)
                        extracted_metadata_ai = ai_response_json
                        if extracted_metadata_ai.get("error_page_detected") is True: # Explicit check for boolean True
                             active_logger.warning(f"LLM detected non-article page for {main_url}. Metadata: {extracted_metadata_ai}")
                    except json.JSONDecodeError:
                        active_logger.warning(f"LLM response for {main_url} was not clean JSON despite requesting JSON_MIME. Raw: '{candidate_text[:500]}...'. Marking as parse failed.")
                        extracted_metadata_ai = {field: "AI_parse_failed" for field in ai_extraction_schema}
                else:
                    active_logger.warning(f"LLM returned no valid candidate for {main_url}.")
                    extracted_metadata_ai = {field: "AI_no_candidate" for field in ai_extraction_schema}
                
                active_logger.info(f"AI extracted metadata for {main_url}: {extracted_metadata_ai}")

            except Exception as e_llm:
                active_logger.error(f"LLM metadata extraction failed for {main_url}: {str(e_llm)}", exc_info=True)
                extracted_metadata_ai = {field: "AI_extraction_error" for field in ai_extraction_schema}
        else:
            active_logger.info("No fields configured for AI extraction in 'metadata_extraction_fields_config' (excluding Law-ID if computed).")


        # --- 3. Prepare Firestore Data ---
        final_firestore_data = {
            "main_url": main_url,
            "identifier": identifier,
            "category_url": category_url,
            "processing_status": "metadata_extracted",
            "scrape_timestamp": firestore.SERVER_TIMESTAMP,
            "last_updated": firestore.SERVER_TIMESTAMP,
            "ai_metadata_extraction_successful": bool(extracted_metadata_ai and 
                                                   not any(isinstance(val, str) and val.startswith("AI_") for val in extracted_metadata_ai.values()) and
                                                   not extracted_metadata_ai.get("error_page_detected")),
            **category_page_metadata # Merges fields like 'category_name' from discover_main_urls
        }

        for key, value in extracted_metadata_ai.items():
            if key == "error_page_detected": # Don't store this control field as metadata
                continue
            final_firestore_data[sanitize_field_name(key)] = value # Sanitize for Firestore field names
        
        # Handle "Law-ID"
        # Check if "Law-ID" is in the original list of fields intended for the item, not just AI schema
        # fields_to_extract_config could also contain non-AI fields like computed ones
        if any(f.lower() == "law-id" for f in fields_to_extract_config if isinstance(f, str)):
            mapping_law_id = project_config.get("field_mappings", {}).get("Law-ID", {})
            if mapping_law_id.get("type") == "computed" and "generate_law_id" in mapping_law_id.get("source", ""):
                sequence_collection_name = f"{firestore_collection_name}_metadata" # Suffix for metadata like sequences
                sequence_doc_name = mapping_law_id.get("sequence_doc_name", f"sequence_law_id_{project_config_name}") # Project-specific sequence
                sequence_doc_ref = db.collection(sequence_collection_name).document(sequence_doc_name)
                
                current_sequence_number = 0 # Default if no sequence doc exists
                try:
                    db_transaction = db.transaction()
                    
                    # Read current sequence within transaction for initial value, then pass to transactional update
                    # This logic is simplified; update_sequence_number_transactional handles the atomic increment better.
                    # We need to determine the *next* sequence number to propose.
                    
                    # Initial read (outside transaction or first step in a more complex one)
                    # to estimate next sequence number.
                    # The transactional function will confirm and prevent race conditions.
                    seq_snap_for_next = sequence_doc_ref.get()
                    if seq_snap_for_next.exists:
                        proposed_next_sequence = seq_snap_for_next.to_dict().get("last_sequence", 0) + 1
                    else:
                        proposed_next_sequence = 1

                    current_sequence_number = update_sequence_number_transactional(db_transaction, sequence_doc_ref, proposed_next_sequence)

                    abbreviation_source_field = mapping_law_id.get("abbreviation_source_field")
                    abbreviation_value = final_firestore_data.get(abbreviation_source_field) if abbreviation_source_field else project_config_name

                    final_firestore_data["Law-ID"] = generate_law_id(
                        project_config_name=project_config_name, # Original project name for context
                        sequence_number=current_sequence_number,
                        abbreviation=abbreviation_value if abbreviation_source_field else None # Pass specific abbreviation if available
                    )
                    active_logger.info(f"Generated Law-ID '{final_firestore_data['Law-ID']}' using sequence {current_sequence_number} for {main_url}")
                except Exception as e_seq:
                    active_logger.error(f"Failed to get or update sequence number for Law-ID: {str(e_seq)}. Law-ID will be 'Not Available_SeqError'.", exc_info=True)
                    final_firestore_data["Law-ID"] = "Not Available_SeqError"
            elif "Law-ID" not in final_firestore_data: # If not computed and not from AI (and was expected)
                 final_firestore_data["Law-ID"] = "Not Available_Cfg"


        # --- 4. Store in Firestore ---
        doc_ref = db.collection(firestore_collection_name).document(identifier)
        
        for attempt in range(MAX_FIRESTORE_RETRIES):
            try:
                doc_ref.set(final_firestore_data, merge=True)
                active_logger.info(f"Successfully stored/merged metadata for {identifier} (URL: {main_url}) to Firestore.")
                break
            except Exception as e_fs_set:
                active_logger.warning(f"Firestore set failed for {identifier} on attempt {attempt + 1}: {str(e_fs_set)}")
                if attempt < MAX_FIRESTORE_RETRIES - 1:
                    time.sleep(RETRY_BACKOFF * (2**attempt))
                else:
                    active_logger.error(f"Max retries reached for Firestore set for {identifier}: {str(e_fs_set)}. Raising.")
                    raise

        # --- 5. Trigger Next Step (fetch-content) ---
        pub_payload_fetch_content = {
            "customer": customer,
            "project": project_config_name,
            "identifier": identifier,
            "main_url": main_url
        }
        
        output_topic_path = publisher_client.topic_path(gcp_project_id, NEXT_STEP_FETCH_CONTENT_TOPIC_NAME)
        future = publisher_client.publish(
            output_topic_path,
            json.dumps(pub_payload_fetch_content).encode('utf-8')
        )
        future.result() # Wait for publish to complete
        active_logger.info(f"Published to '{NEXT_STEP_FETCH_CONTENT_TOPIC_NAME}' for {identifier}. Message ID: {future.message_id}")

        return {"status": "success", "identifier": identifier, "main_url": main_url}

    except ValueError as ve: # Catch config/setup errors specifically
        active_logger.error(f"Configuration or input error in extract_initial_metadata: {str(ve)}", exc_info=True)
        return {"status": "error", "message": str(ve)}
    except Exception as e:
        # Use active_logger if initialized, otherwise global logger
        current_logger = active_logger if 'active_logger' in locals() and active_logger != logger else logger
        current_logger.error(f"Critical error in extract_initial_metadata for Pub/Sub message {pubsub_message if pubsub_message else 'unknown'}: {str(e)}", exc_info=True)
        raise # Re-raise for Pub/Sub retries or dead-lettering