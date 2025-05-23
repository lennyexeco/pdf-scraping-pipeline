import json
import base64
import logging
import re
import os
import gzip
import xml.etree.ElementTree as ET
from xml.dom import minidom
from google.cloud import firestore, pubsub_v1, storage, aiplatform
from google.cloud.aiplatform.gapic import PredictionServiceClient
from google.api_core import exceptions
from src.common.utils import generate_url_hash, setup_logging, compress_and_upload
from src.common.config import load_customer_config, load_project_config, get_secret
from datetime import datetime
import functions_framework
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)
RETRY_TOPIC_NAME = "retry-pipeline"
MAX_RETRIES = 3
APIFY_BATCH_SIZE = int(os.environ.get("APIFY_BATCH_SIZE", 50))

# --- Configuration for LLM Model ---
LLM_MODEL_ID = "gemini-2.0-flash-lite-001" # Switched to Flash Lite as requested

def extract_title_from_html(content):
    """Extract title from HTML content."""
    try:
        soup = BeautifulSoup(content, 'html.parser')
        h1 = soup.find('h1', class_='jnlangue') or soup.find('h1')
        if h1:
            return h1.get_text(strip=True)
        title_tag = soup.find('title')
        if title_tag:
            return title_tag.get_text(strip=True)
        return "untitled"
    except Exception as e:
        logger.warning(f"Error extracting title from HTML: {e}")
        return "untitled"

def sanitize_error_message(error_message):
    """Remove sensitive data from error messages."""
    sanitized = re.sub(r'https?://\S+', '[URL]', error_message)
    sanitized = re.sub(r'[0-9a-f]{32}', '[ID]', sanitized) # Example for MD5-like IDs
    # Add more sanitization rules as needed
    return sanitized

def get_mapped_field(item, field, field_mappings, llm_client=None, llm_endpoint=None, gcp_project_id=None, logger_instance=None):
    """Get the value for a field based on its mapping in field_mappings."""
    current_logger = logger_instance if logger_instance else logger
    mapping = field_mappings.get(field, {})
    source = mapping.get("source", field)
    field_type = mapping.get("type", "direct")

    if field_type == "computed":
        if "generate_url_hash" in source:
            url_fields = source.replace("generate_url_hash(", "").replace(")", "").split(" || ")
            for url_field in url_fields:
                url_field = url_field.strip()
                if item.get(url_field):
                    return generate_url_hash(item[url_field])
            return generate_url_hash(f"no-id-{datetime.now().isoformat()}")
        elif "split('/').last.replace('.html', '')" in source:
            # Safely extract the field name to parse
            match = re.search(r"([a-zA-Z0-9_]+)\.split", source)
            if match:
                url_field_to_parse = match.group(1)
                if item.get(url_field_to_parse):
                    try:
                        return item[url_field_to_parse].split("/")[-1].replace(".html", "")
                    except Exception:
                        return "" # Or some other default
            return "" # Or some other default
        return "Not Available"
    else: # Direct mapping
        source_fields = source.split(" || ")
        for source_field in source_fields:
            source_field = source_field.strip()
            if source_field.startswith("extract_title_from_html"):
                content_fields = source_field.replace("extract_title_from_html(", "").replace(")", "").split(" || ")
                for content_field in content_fields:
                    content_field = content_field.strip()
                    if item.get(content_field):
                        return extract_title_from_html(item[content_field])
                return "untitled"
            if item.get(source_field) is not None: # Check for not None to allow empty strings if they are valid values
                return item[source_field]
        
        # LLM fallback if direct mapping fails
        if llm_client and llm_endpoint and gcp_project_id:
            current_logger.info(f"No direct value for '{field}', attempting LLM suggestion.")
            prompt = f"""
            The dataset item has the following keys and a sample of their values:
            { {k: str(v)[:100] + '...' if isinstance(v, str) and len(v) > 100 else v for k, v in item.items()} }

            From the available data in the item, what would be the most appropriate value for the field named '{field}'?
            If you can identify a direct field from the item that maps well to '{field}', use its value.
            If not, and if '{field}' implies a common concept like 'Title' or 'Summary' that might be derivable from other text fields, try to derive it.
            If a suitable value cannot be found or derived, output "Not Available".

            Return a JSON object with a single key "suggested_value":
            ```json
            {{"suggested_value": "The value you determined for the field '{field}'"}}
            ```
            """
            try:
                instances = [{"content": prompt}] # Updated payload for Gemini
                parameters = {"temperature": 0.7, "maxOutputTokens": 256, "topP": 0.8, "topK": 40}
                
                current_logger.debug(f"Sending to LLM for field '{field}'. Endpoint: {llm_endpoint}. Prompt: {prompt[:200]}...")
                response = llm_client.predict(endpoint=llm_endpoint, instances=instances, parameters=parameters)
                
                if response.predictions:
                    prediction_text = response.predictions[0].get('content') # Correct for Gemini
                    if prediction_text:
                        current_logger.debug(f"LLM raw response for '{field}': {prediction_text[:200]}")
                        suggestion_json = None
                        match_json = re.search(r"```json\s*([\s\S]*?)\s*```", prediction_text, re.MULTILINE | re.DOTALL)
                        if match_json:
                            try:
                                suggestion_json = json.loads(match_json.group(1))
                            except json.JSONDecodeError as e:
                                current_logger.warning(f"LLM: Failed to parse JSON from markdown for '{field}': {e}. Raw: {match_json.group(1)}")
                        else:
                            try:
                                suggestion_json = json.loads(prediction_text)
                            except json.JSONDecodeError as e:
                                current_logger.warning(f"LLM: Prediction for '{field}' not in markdown and not valid JSON: {e}. Raw: {prediction_text[:200]}")
                        
                        if suggestion_json and "suggested_value" in suggestion_json:
                            suggested_value = suggestion_json["suggested_value"]
                            if suggested_value != "Not Available":
                                current_logger.info(f"LLM successfully suggested value for '{field}': '{str(suggested_value)[:100]}'")
                                return suggested_value
                            else:
                                current_logger.info(f"LLM suggested 'Not Available' for '{field}'.")
                        else:
                            current_logger.warning(f"LLM suggestion for '{field}' missing 'suggested_value' key or invalid JSON. Response: {prediction_text[:200]}")
                    else:
                        current_logger.warning(f"LLM prediction content empty for '{field}'. Full prediction: {response.predictions[0]}")
                else:
                    current_logger.warning(f"LLM returned no predictions for '{field}'.")
            except Exception as e:
                current_logger.warning(f"LLM field mapping API call failed for {field}: {str(e)}")
        return "Not Available"

def analyze_error_with_vertex_ai(error_message, stage, field_mappings, dataset_fields, gcp_project_id, logger_instance):
    """Analyze error using Vertex AI and suggest retry parameters."""
    current_logger = logger_instance if logger_instance else logger
    try:
        # aiplatform.init is not strictly necessary here if client options are set correctly.
        # aiplatform.init(project=gcp_project_id, location='europe-west1') 
        client_options = {"api_endpoint": "europe-west1-aiplatform.googleapis.com"}
        client = PredictionServiceClient(client_options=client_options)
        # Use the global LLM_MODEL_ID
        endpoint = f"projects/{gcp_project_id}/locations/europe-west1/publishers/google/models/{LLM_MODEL_ID}"
        current_logger.info(f"Analyzing error with LLM: {endpoint}")

        sanitized_error = sanitize_error_message(error_message)

        prompt = f"""
        Analyze the following error from stage '{stage}' in a data processing pipeline:
        Error: {sanitized_error}

        Dataset fields available: {dataset_fields}
        Current field mappings: {json.dumps(field_mappings, indent=2)}

        Suggest retry parameters to resolve the issue. Possible parameters include:
        - batch_size: Adjust batch size for processing (default: {APIFY_BATCH_SIZE})
        - truncate_content: Truncate content to avoid size limits (boolean)
        - recheck_metadata: Recheck Firestore metadata (boolean)
        - field_remapping: Suggest new field mappings if the error relates to missing or incorrect fields (e.g., {{"URL": "new_field_name"}})

        Return ONLY a JSON object with suggested parameters. Example:
        ```json
        {{"adjusted_params": {{"batch_size": 10, "truncate_content": true, "recheck_metadata": false, "field_remapping": {{"URL": "pageUrl"}}}}}}
        ```
        """

        instances = [{"content": prompt}] # Updated payload for Gemini
        parameters = {"temperature": 0.7, "maxOutputTokens": 300, "topP": 0.8, "topK": 40}
        response = client.predict(endpoint=endpoint, instances=instances, parameters=parameters)

        if not response.predictions:
            current_logger.error("Vertex AI returned no predictions for error analysis.")
            return {"adjusted_params": {"batch_size": APIFY_BATCH_SIZE}} # Default fallback

        prediction_content = response.predictions[0].get('content') # Correct for Gemini
        if not prediction_content:
            current_logger.warning("Vertex AI prediction content is empty for error analysis.")
            return {"adjusted_params": {"batch_size": APIFY_BATCH_SIZE}}

        try:
            match = re.search(r"```json\s*([\s\S]*?)\s*```", prediction_content, re.MULTILINE | re.DOTALL)
            json_str = match.group(1) if match else prediction_content
            parsed_response = json.loads(json_str)
            current_logger.info(f"Vertex AI suggested for error: {parsed_response}")
            # Ensure 'adjusted_params' key exists, even if empty
            if "adjusted_params" not in parsed_response:
                parsed_response["adjusted_params"] = {}
            return parsed_response
        except json.JSONDecodeError as e:
            current_logger.warning(f"Vertex AI response for error analysis is not valid JSON: '{prediction_content}'. Error: {e}")
            return {"adjusted_params": {"batch_size": APIFY_BATCH_SIZE}} # Default fallback
    except exceptions.GoogleAPIError as e:
        current_logger.error(f"Vertex AI API error during error analysis: {str(e)}")
        # Simple rule-based fallbacks
        if "size exceeds" in error_message.lower():
            return {"adjusted_params": {"batch_size": 1, "truncate_content": True}}
        elif "no document to update" in error_message.lower(): # Firestore specific
            return {"adjusted_params": {"batch_size": APIFY_BATCH_SIZE, "recheck_metadata": True}} # Keep batch size or adjust as needed
        return {"adjusted_params": {"batch_size": APIFY_BATCH_SIZE}} # Default fallback
    except Exception as e:
        current_logger.error(f"Error in Vertex AI analysis function: {str(e)}")
        return {"adjusted_params": {"batch_size": APIFY_BATCH_SIZE}} # Default fallback

@functions_framework.cloud_event
def retry_pipeline(cloud_event):
    active_logger = logger # Start with module logger
    try:
        # Ensure data is present (common check for Pub/Sub triggered functions)
        if not cloud_event.data or not cloud_event.data.get("message") or not cloud_event.data["message"].get("data"):
            # Log the entire cloud_event.data for better debugging if the structure is unexpected
            active_logger.error(f"Invalid Pub/Sub message structure in cloud_event.data: {cloud_event.data}")
            raise ValueError("No 'data' field in Pub/Sub message or message structure is incorrect.")
        
        pubsub_message_data_encoded = cloud_event.data["message"]["data"]
        data = json.loads(base64.b64decode(pubsub_message_data_encoded).decode("utf-8"))
        active_logger.info(f"Decoded retry message: {data}")

        customer_id = data.get("customer")
        project_id_config_name = data.get("project") # Assuming this is the project config name
        dataset_id = data.get("dataset_id")
        dataset_type = data.get("dataset_type", "items")
        identifier = data.get("identifier")
        stage = data.get("stage")
        retry_count = int(data.get("retry_count", 0))

        if not all([customer_id, project_id_config_name, dataset_id, identifier, stage]): # dataset_type can default
            active_logger.error(f"Missing required fields in retry message: customer={customer_id}, project={project_id_config_name}, dataset_id={dataset_id}, identifier={identifier}, stage={stage}")
            raise ValueError("Missing required fields in retry message")

        # Setup customer/project specific logging
        active_logger = setup_logging(customer_id, project_id_config_name)
        active_logger.info(f"Processing retry for identifier '{identifier}', stage '{stage}', retry count {retry_count}.")

        customer_config = load_customer_config(customer_id)
        project_config = load_project_config(project_id_config_name)

        gcp_project_id = customer_config.get("gcp_project_id", os.environ.get("GCP_PROJECT"))
        if not gcp_project_id:
            active_logger.error("GCP Project ID not found in customer_config or GCP_PROJECT env var.")
            raise ValueError("GCP Project ID not configured.")

        firestore_collection_name = project_config.get("firestore_collection")
        search_results_collection_name = project_config.get("search_results_collection", f"{firestore_collection_name}_search_results")
        firestore_db_id = project_config.get("firestore_database_id") # Could be None for default DB
        gcs_bucket_name = project_config.get("gcs_bucket")
        required_fields = project_config.get("required_fields", [])
        search_required_fields = project_config.get("search_required_fields", [])
        field_mappings = project_config.get("field_mappings", {})
        error_collection_name = f"{firestore_collection_name}_errors" # Corrected variable name

        db_options = {"project": gcp_project_id}
        if firestore_db_id:
            db_options["database"] = firestore_db_id
        db = firestore.Client(**db_options)
        
        storage_client = storage.Client(project=gcp_project_id)
        bucket = storage_client.bucket(gcs_bucket_name)
        publisher = pubsub_v1.PublisherClient()
        # Use the global RETRY_TOPIC_NAME for consistency
        retry_topic_path = publisher.topic_path(gcp_project_id, RETRY_TOPIC_NAME)


        # Initialize Vertex AI client for LLM calls
        # aiplatform.init is not strictly necessary here if client options are set correctly.
        # aiplatform.init(project=gcp_project_id, location='europe-west1')
        client_options_llm = {"api_endpoint": "europe-west1-aiplatform.googleapis.com"} # Assuming europe-west1 for all LLM calls
        llm_client = PredictionServiceClient(client_options=client_options_llm)
        # Use the global LLM_MODEL_ID
        llm_endpoint_str = f"projects/{gcp_project_id}/locations/europe-west1/publishers/google/models/{LLM_MODEL_ID}"
        active_logger.info(f"LLM endpoint for retry processing: {llm_endpoint_str}")

        error_doc_ref = db.collection(error_collection_name).document(identifier)
        error_doc = error_doc_ref.get()

        if not error_doc.exists:
            active_logger.error(f"No error document found for identifier '{identifier}'. Cannot proceed with retry.")
            return {"status": "error", "message": "No error document found for identifier"}

        error_data = error_doc.to_dict()
        # Prioritize 'item_data_snapshot' if available from a previous retry attempt, else 'original_item'
        original_item = data.get("item_data_snapshot") or error_data.get("original_item") or error_data.get("original_blob")
        
        error_message = error_data.get("error", "No error message recorded in error document.")
        # Ensure dataset_fields are derived from the actual item being retried
        dataset_fields = list(original_item.keys()) if isinstance(original_item, dict) else []


        if retry_count >= MAX_RETRIES:
            active_logger.error(f"Max retries ({MAX_RETRIES}) reached for identifier '{identifier}' at stage '{stage}'. Marking as Unresolvable.")
            error_doc_ref.update({"status": "Unresolvable", "last_attempt_timestamp": firestore.SERVER_TIMESTAMP, "final_error_message": error_message})
            return {"status": "failed", "message": f"Max retries reached for {identifier}"}

        # Call LLM for error analysis
        llm_suggestion = analyze_error_with_vertex_ai(error_message, stage, field_mappings, dataset_fields, gcp_project_id, active_logger)
        adjusted_params = llm_suggestion.get("adjusted_params", {}) # Ensure adjusted_params is always a dict
        
        # Extract parameters from LLM suggestion or use defaults
        # batch_size = adjusted_params.get("batch_size", APIFY_BATCH_SIZE) # Batch size might not be relevant for single item retry
        truncate_content_flag = adjusted_params.get("truncate_content", False)
        recheck_metadata_flag = adjusted_params.get("recheck_metadata", False)
        field_remapping_suggestion = adjusted_params.get("field_remapping", {})
        active_logger.info(f"LLM suggested params: truncate_content={truncate_content_flag}, recheck_metadata={recheck_metadata_flag}, field_remapping={field_remapping_suggestion}")

        # Update field mappings based on LLM suggestions for this attempt
        current_field_mappings = field_mappings.copy() # Start with base config
        if isinstance(field_remapping_suggestion, dict):
            for field, new_source in field_remapping_suggestion.items():
                if isinstance(new_source, str): # Basic check for new source string
                    current_field_mappings[field] = {"source": new_source, "type": "direct"} # Assuming direct mapping for simplicity
                    active_logger.info(f"Temporarily updated field mapping for this retry: {field} -> {new_source}")
                else:
                    active_logger.warning(f"LLM suggested invalid new_source '{new_source}' for field '{field}'. Skipping this remapping.")
        
        # Log the LLM suggestion and attempt details to Firestore
        error_doc_ref.update({
            "llm_suggestion_history": firestore.ArrayUnion([llm_suggestion]),
            "last_attempt_timestamp": firestore.SERVER_TIMESTAMP,
            "retry_attempt_details": firestore.ArrayUnion([{
                "timestamp": firestore.SERVER_TIMESTAMP,
                "retry_count": retry_count + 1, # This is the current attempt number
                "stage": stage,
                "llm_suggestion": llm_suggestion,
                "applied_params": {
                    "truncate_content": truncate_content_flag,
                    "recheck_metadata": recheck_metadata_flag,
                    "field_remapping": field_remapping_suggestion
                }
            }])
        })

        next_retry_count_for_pubsub = retry_count + 1

        def publish_for_next_retry_attempt(current_error_msg="Retry attempt failed", item_snapshot=None):
            error_doc_ref.update({
                "retry_count": next_retry_count_for_pubsub, # This will be the count for the *next* attempt
                "error": f"Retry attempt {retry_count + 1} for stage '{stage}' failed: {sanitize_error_message(current_error_msg)}",
                "status": "Retrying"
            })
            retry_message_payload = {
                "customer": customer_id,
                "project": project_id_config_name,
                "dataset_id": dataset_id,
                "dataset_type": dataset_type,
                "identifier": identifier,
                "stage": stage,
                "retry_count": next_retry_count_for_pubsub,
                "item_data_snapshot": item_snapshot if item_snapshot else {k: str(v)[:200] + '...' if isinstance(v, str) and len(v) > 200 else v for k,v in original_item.items()} if original_item else {}
            }
            message_bytes = json.dumps(retry_message_payload).encode('utf-8')
            publisher.publish(retry_topic_path, message_bytes)
            active_logger.info(f"Published subsequent retry message for identifier '{identifier}', stage '{stage}', next attempt count {next_retry_count_for_pubsub}.")


        # --- Stage-specific retry logic ---
        if stage == "extract_metadata":
            if not original_item:
                active_logger.error(f"Cannot retry 'extract_metadata' for '{identifier}': original_item data is missing.")
                error_doc_ref.update({"status": "Unresolvable", "error": "Missing original_item for extract_metadata retry."})
                return {"status": "failed", "message": "Missing original_item data for extract_metadata retry."}
            try:
                metadata = {
                    # "raw_data": original_item, # Avoid storing full raw_data again if it's huge
                    "scrape_date": original_item.get("extractionTimestamp", firestore.SERVER_TIMESTAMP),
                    "processed_at_extract_metadata_retry": firestore.SERVER_TIMESTAMP,
                    "retry_attempt_count": retry_count + 1
                }
                target_collection_name = search_results_collection_name if dataset_type == "search_results" else firestore_collection_name
                fields_to_process_now = search_required_fields if dataset_type == "search_results" else required_fields

                for field_name in fields_to_process_now:
                    metadata[field_name] = get_mapped_field(original_item, field_name, current_field_mappings, llm_client, llm_endpoint_str, gcp_project_id, active_logger)

                # LLM Enhancement for fields still "Not Available"
                llm_fields_to_enhance_retry = [f_name for f_name in fields_to_process_now if metadata.get(f_name) in ["Not Available", "untitled", ""]]
                if llm_fields_to_enhance_retry:
                    content_for_llm_retry = metadata.get("Content") or original_item.get("htmlContent") or original_item.get("text") or original_item.get("body") or ""
                    prompt_llm_enhance_retry = f"""
                    Analyze the following content and suggest values for the missing metadata fields: {llm_fields_to_enhance_retry}.
                    Content (first 1000 chars): {str(content_for_llm_retry)[:1000]}
                    Current Metadata (relevant fields): { {f_name: metadata.get(f_name) for f_name in fields_to_process_now} }
                    Original item has keys: {list(original_item.keys())}
                    Return ONLY a JSON object with the suggested values for the missing fields. Example: {{"Title": "Suggested Title"}}
                    """
                    try:
                        instances_llm_enhance_retry = [{"content": prompt_llm_enhance_retry}]
                        parameters_llm_enhance_retry = {"temperature": 0.5, "maxOutputTokens": 512, "topP": 0.8, "topK": 40}
                        response_llm_enhance_retry = llm_client.predict(endpoint=llm_endpoint_str, instances=instances_llm_enhance_retry, parameters=parameters_llm_enhance_retry)
                        if response_llm_enhance_retry.predictions:
                            prediction_text_enhance_retry = response_llm_enhance_retry.predictions[0].get('content')
                            if prediction_text_enhance_retry:
                                llm_suggestions_enhance_retry = None
                                match_enhance_retry = re.search(r"```json\s*([\s\S]*?)\s*```", prediction_text_enhance_retry, re.MULTILINE | re.DOTALL)
                                if match_enhance_retry: llm_suggestions_enhance_retry = json.loads(match_enhance_retry.group(1))
                                else: llm_suggestions_enhance_retry = json.loads(prediction_text_enhance_retry)
                                
                                if llm_suggestions_enhance_retry:
                                    for field_to_enhance in llm_fields_to_enhance_retry:
                                        if field_to_enhance in llm_suggestions_enhance_retry and llm_suggestions_enhance_retry[field_to_enhance] not in ["Not Available", "untitled", ""]:
                                            metadata[field_to_enhance] = llm_suggestions_enhance_retry[field_to_enhance]
                                            active_logger.info(f"LLM retry enhancement for '{field_to_enhance}' on {identifier}. New: '{str(metadata[field_to_enhance])[:100]}'")
                    except Exception as e_llm_retry:
                        active_logger.warning(f"LLM enhancement during retry failed for {identifier}: {str(e_llm_retry)}")
                
                # Size check and GCS upload if oversized
                serialized_metadata_str = json.dumps(metadata, default=str) # Add raw_data back if needed and feasible
                if "raw_data" not in metadata and len(serialized_metadata_str) < 800_000 : # Add back original_item if space permits
                    metadata_with_raw = metadata.copy()
                    metadata_with_raw["raw_data_snapshot_from_retry"] = original_item
                    test_serialized_size = len(json.dumps(metadata_with_raw, default=str).encode('utf-8'))
                    if test_serialized_size <= 1_000_000:
                       serialized_metadata_str = json.dumps(metadata_with_raw, default=str)

                serialized_size = len(serialized_metadata_str.encode('utf-8'))

                if serialized_size > 1_000_000:
                    active_logger.warning(f"Retried metadata for '{identifier}' still exceeds 1MB ({serialized_size}), uploading full original_item to GCS.")
                    gcs_path_oversized_retry = f"oversized_retry/{project_id_config_name}/{dataset_id}/{identifier}.json.gz"
                    compress_and_upload(json.dumps(original_item, default=str), gcs_bucket_name, gcs_path_oversized_retry)
                    
                    # Create a minimal Firestore document
                    minimal_metadata = {key: val for key, val in metadata.items() if key != "raw_data_snapshot_from_retry"} # Exclude potentially large raw data
                    minimal_metadata["full_item_gcs_path_retry"] = f"gs://{gcs_bucket_name}/{gcs_path_oversized_retry}"
                    minimal_metadata["status_notice_retry"] = "Full item data stored in GCS during retry due to size."
                    minimal_metadata["processed_at_extract_metadata_retry"] = firestore.SERVER_TIMESTAMP # Ensure this is updated
                    minimal_metadata["retry_attempt_count"] = retry_count + 1

                    doc_ref = db.collection(target_collection_name).document(identifier)
                    doc_ref.set(minimal_metadata, merge=True) # Merge to preserve other fields if any
                else:
                    doc_ref = db.collection(target_collection_name).document(identifier)
                    # If metadata was updated successfully, set it (potentially with raw data if included)
                    doc_ref.set(json.loads(serialized_metadata_str), merge=True)


                error_doc_ref.update({"status": "Resolved", "resolved_at_timestamp": firestore.SERVER_TIMESTAMP, "resolution_details": "Successfully processed at extract_metadata stage during retry."})
                active_logger.info(f"Successfully retried 'extract_metadata' for identifier '{identifier}'.")
                return {"status": "success", "identifier": identifier, "stage": stage, "message": "extract_metadata retried successfully"}

            except Exception as e:
                active_logger.error(f"Retry attempt {retry_count + 1} failed for '{identifier}' at 'extract_metadata': {str(e)}", exc_info=True)
                publish_for_next_retry_attempt(str(e), original_item)
                return {"status": "retry_queued", "identifier": identifier, "stage": stage, "message": f"extract_metadata retry failed: {str(e)}"}

        elif stage == "store_html":
            if not original_item: # 'original_item' here should be the Apify item containing HTML content or URL to fetch
                active_logger.error(f"Cannot retry 'store_html' for '{identifier}': original_item data is missing.")
                error_doc_ref.update({"status": "Unresolvable", "error": "Missing original_item for store_html retry."})
                return {"status": "failed", "message": "Missing original_item data for store_html retry."}
            try:
                target_collection_name = search_results_collection_name if dataset_type == "search_results" else firestore_collection_name
                doc_ref = db.collection(target_collection_name).document(identifier)
                doc_snapshot = doc_ref.get()

                if not doc_snapshot.exists and not recheck_metadata_flag:
                    active_logger.error(f"No Firestore document found for '{identifier}' during store_html retry. Cannot proceed without recheck_metadata_flag.")
                    # Optionally, if no doc, could queue for extract_metadata again
                    error_doc_ref.update({"status": "Unresolvable", "error": "Firestore document not found for store_html and recheck_metadata not flagged."})
                    return {"status": "failed", "message": f"No Firestore document for {identifier} and recheck_metadata_flag is false."}
                
                if not doc_snapshot.exists and recheck_metadata_flag:
                    active_logger.warning(f"No Firestore document for '{identifier}' during store_html retry, but recheck_metadata_flag is true. Queuing for extract_metadata stage.")
                    publish_for_next_retry_attempt("Document not found, recheck_metadata suggests retrying extract_metadata.", original_item) # Stage should be extract_metadata
                    # Update the current error doc to reflect this decision before exiting
                    error_doc_ref.update({
                        "status": "Retrying", 
                        "error": "store_html retry: Doc not found, re-queued for extract_metadata.",
                        "retry_count": retry_count # Keep current retry_count as this is a stage change
                    })
                    return {"status": "retry_queued", "identifier": identifier, "stage": "extract_metadata", "message": "Re-queued for extract_metadata stage."}

                doc_data = doc_snapshot.to_dict()
                # Try to get content directly from original_item, then fallback to what might be in doc_data
                # Use current_field_mappings which might have been updated by LLM
                content_to_store = get_mapped_field(original_item, "Content", current_field_mappings, llm_client, llm_endpoint_str, gcp_project_id, active_logger)
                if content_to_store == "Not Available" and "Content" in doc_data : # Fallback to existing doc_data if mapping fails
                    content_to_store = doc_data.get("Content")

                # URL derivation - prioritize original_item with current_field_mappings
                url_for_html = doc_data.get("URL") # Fallback to existing URL in Firestore doc
                mapped_url = get_mapped_field(original_item, "URL", current_field_mappings, llm_client, llm_endpoint_str, gcp_project_id, active_logger)
                if mapped_url != "Not Available":
                    url_for_html = mapped_url


                if not url_for_html or url_for_html == "Not Available" or not content_to_store or content_to_store == "Not Available":
                    active_logger.error(f"Invalid URL or content for '{identifier}' during store_html retry. URL: '{url_for_html}', Content available: {content_to_store != 'Not Available'}")
                    error_doc_ref.update({"status": "Unresolvable", "error": "Invalid URL or content during store_html retry."})
                    return {"status": "failed", "message": "Invalid URL or content for store_html retry."}

                if truncate_content_flag and isinstance(content_to_store, str):
                    content_to_store = content_to_store[:1_000_000] # Example truncation limit for GCS
                    active_logger.info(f"Truncated content to 1,000,000 bytes for '{identifier}' during store_html retry.")

                gcs_destination_path = f"{'search_results' if dataset_type == 'search_results' else 'pending'}/{project_id_config_name}/{datetime.now().strftime('%Y%m%d')}/{identifier}.{'txt' if dataset_type == 'search_results' else 'html.gz'}"
                
                if dataset_type == "search_results": # Store as plain text for search results
                    blob_to_upload = bucket.blob(gcs_destination_path)
                    blob_to_upload.upload_from_string(content_to_store, content_type="text/plain; charset=utf-8")
                else: # Compress for regular HTML
                    compress_and_upload(content_to_store, gcs_bucket_name, gcs_destination_path, storage_client_override=storage_client)


                doc_ref.update({
                    "html_path": f"gs://{gcs_bucket_name}/{gcs_destination_path}",
                    "html_stored_at_retry": firestore.SERVER_TIMESTAMP,
                    "retry_attempt_count": retry_count + 1
                })
                error_doc_ref.update({"status": "Resolved", "resolved_at_timestamp": firestore.SERVER_TIMESTAMP, "resolution_details": "Successfully stored HTML during retry."})
                active_logger.info(f"Successfully retried 'store_html' for identifier '{identifier}'. Path: gs://{gcs_bucket_name}/{gcs_destination_path}")
                return {"status": "success", "identifier": identifier, "stage": stage, "message": "store_html retried successfully"}

            except Exception as e:
                active_logger.error(f"Retry attempt {retry_count + 1} failed for '{identifier}' at 'store_html': {str(e)}", exc_info=True)
                publish_for_next_retry_attempt(str(e), original_item)
                return {"status": "retry_queued", "identifier": identifier, "stage": stage, "message": f"store_html retry failed: {str(e)}"}
        
        # Add other stages like fix_image_urls and generate_xml similarly,
        # ensuring they use current_field_mappings and adjusted_params from LLM.

        else: # Fallback for unsupported stages
            active_logger.error(f"Unsupported stage '{stage}' for retry for identifier '{identifier}'.")
            error_doc_ref.update({"status": "Unresolvable", "error": f"Unsupported stage for retry: {stage}"})
            return {"status": "failed", "message": f"Unsupported stage for retry: {stage}"}

    except ValueError as ve: # Catch config errors or missing data early
        active_logger.error(f"ValueError in retry_pipeline: {str(ve)}", exc_info=True)
        # No Pub/Sub re-publish here as it's likely a config or input message issue.
        return {"status": "error", "message": f"ValueError: {str(ve)}"}
    except Exception as e: # General catch-all for unexpected errors
        active_logger.error(f"Critical unhandled error in retry_pipeline: {str(e)}", exc_info=True)
        # If an identifier is available, try to update its error doc, otherwise, it's a broader failure.
        if 'identifier' in locals() and 'error_doc_ref' in locals() and error_doc_ref:
            try:
                error_doc_ref.update({"status": "Failed_Critical", "error": f"Critical unhandled error: {sanitize_error_message(str(e))}"})
            except Exception as e_firestore_update:
                active_logger.error(f"Failed to update error document during critical failure: {e_firestore_update}")
        # Do not re-publish here as it might cause an infinite loop for critical errors.
        return {"status": "error", "message": f"Critical error: {str(e)}"}