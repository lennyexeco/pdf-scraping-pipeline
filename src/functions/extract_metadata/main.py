import json
import base64
import logging
import re
import os
from google.cloud import firestore, pubsub_v1, storage, aiplatform
from google.cloud.aiplatform.gapic import PredictionServiceClient
from apify_client import ApifyClient
from src.common.utils import generate_url_hash, compress_and_upload, setup_logging
from src.common.config import load_customer_config, load_project_config, get_secret
from datetime import datetime
from bs4 import BeautifulSoup
# Removed: import functions_framework (not needed for Gen1 if not using its decorators explicitly)
import time

logger = logging.getLogger(__name__) # Module-level logger
MAX_ITEMS_PER_INVOCATION = int(os.environ.get("MAX_ITEMS_PER_INVOCATION", 250))
APIFY_BATCH_SIZE = int(os.environ.get("APIFY_BATCH_SIZE", 50))
SELF_TRIGGER_TOPIC_NAME = "process-data"
NEXT_STEP_TOPIC_NAME = "store-html"
RETRY_TOPIC_NAME = "retry-pipeline"
MAX_API_RETRIES = 3
RETRY_BACKOFF = 5  # seconds

# --- Configuration for LLM Model ---
# Ensure this is the exact model ID string from Model Garden for your region
# For Gemini, it's often "gemini-1.0-pro", "gemini-1.5-flash", or a versioned one like "gemini-1.0-pro-002"
# For PaLM, it would be "text-bison@002" or similar
LLM_MODEL_ID = "gemini-2.0-flash-lite-001" # Updated as per request

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
        # Use the module-level logger if active_logger isn't defined in this scope
        (logger_instance if 'logger_instance' in locals() and logger_instance else logger).warning(f"Error extracting title from content: {e}")
        return "untitled"

def generate_identifier(item, field_mappings):
    """Generate a unique identifier for an item using mapped fields."""
    id_mapping = field_mappings.get("Law-ID", {})
    source_str = id_mapping.get("source", "htmlUrl || url || mainUrl")
    url_fields = source_str.split(" || ")

    for field in url_fields:
        field = field.strip()
        if item.get(field):
            return generate_url_hash(item[field])
    return generate_url_hash(f"no-id-{datetime.now().isoformat()}")

def get_mapped_field(item, field, field_mappings, llm_client=None, llm_endpoint_str=None, gcp_project_id=None, logger_instance=None):
    """Get the value for a field based on its mapping in field_mappings."""
    mapping = field_mappings.get(field, {})
    source = mapping.get("source", field)
    field_type = mapping.get("type", "direct")
    
    # Ensure logger_instance is valid, otherwise use global logger
    current_logger = logger_instance if logger_instance else logger

    if field_type == "computed":
        if "generate_url_hash" in source:
            inner_source = source.replace("generate_url_hash(", "").replace(")", "")
            url_fields = inner_source.split(" || ")
            for url_field in url_fields:
                url_field = url_field.strip()
                if item.get(url_field):
                    return generate_url_hash(item[url_field])
            return generate_url_hash(f"no-id-{datetime.now().isoformat()}")
        elif "split('/').last.replace('.html', '')" in source:
            url_field_to_parse = source.split(".split")[0].strip()
            if item.get(url_field_to_parse):
                try:
                    return item[url_field_to_parse].split("/")[-1].replace(".html", "")
                except Exception:
                    return ""
            return ""
        return "Not Available"
    else: # Direct mapping
        source_fields = source.split(" || ")
        for source_field in source_fields:
            source_field = source_field.strip()
            if source_field.startswith("extract_title_from_html"):
                inner_source = source_field.replace("extract_title_from_html(", "").replace(")", "")
                content_fields = inner_source.split(" || ")
                for content_field in content_fields:
                    content_field = content_field.strip()
                    if item.get(content_field):
                        return extract_title_from_html(item[content_field]) # Pass current_logger if that func needs it
                return "untitled"
            if item.get(source_field) is not None:
                return item[source_field]
        
        if llm_client and llm_endpoint_str and gcp_project_id and current_logger:
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
                # --- PAYLOAD FOR GEMINI (if LLM_MODEL_ID is a Gemini model) ---
                instances = [{"content": prompt}]
                # --- PAYLOAD FOR PALM (e.g., text-bison) ---
                # instances = [{"prompt": prompt}] # Uncomment if using PaLM

                parameters = {"temperature": 0.7, "maxOutputTokens": 256, "topP": 0.8, "topK": 40}
                
                current_logger.debug(f"Sending to LLM for field '{field}'. Endpoint: {llm_endpoint_str}. Prompt: {prompt[:200]}...")
                response = llm_client.predict(endpoint=llm_endpoint_str, instances=instances, parameters=parameters)
                
                if response.predictions:
                    prediction_text = response.predictions[0].get('content')
                    if not prediction_text: # Fallback for slightly different Gemini response structures
                        parts = response.predictions[0].get('parts')
                        if parts and isinstance(parts, list) and parts[0].get('text'):
                            prediction_text = parts[0]['text']

                    if prediction_text:
                        current_logger.debug(f"LLM raw response for '{field}': {prediction_text[:200]}")
                        suggestion_json = None
                        # Enhanced JSON extraction from markdown
                        match = re.search(r"```json\s*([\s\S]*?)\s*```", prediction_text, re.MULTILINE | re.DOTALL)
                        if match:
                            try:
                                suggestion_json = json.loads(match.group(1))
                            except json.JSONDecodeError as e:
                                current_logger.warning(f"LLM: Failed to parse JSON from markdown for '{field}': {e}. Raw: {match.group(1)}")
                        else: # Try parsing directly if no markdown block
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
                        current_logger.warning(f"LLM prediction empty for '{field}'. Full prediction: {response.predictions[0]}")
                else:
                    current_logger.warning(f"LLM returned no predictions for '{field}'.")
            except Exception as e:
                current_logger.warning(f"LLM field mapping API call failed for {field}: {str(e)}")
        
        return "Not Available"

# MODIFIED function signature for Gen1
def extract_metadata(event, context):
    active_logger = logger # Use module-level logger initially
    try:
        # MODIFIED: For Gen1, data is in event['data']
        pubsub_message_payload = event.get('data')
        if not pubsub_message_payload:
            active_logger.error(f"No 'data' field in event: {event}")
            raise ValueError("Pub/Sub message data not found in event['data'].")
        
        pubsub_message = json.loads(base64.b64decode(pubsub_message_payload).decode('utf-8'))
        
        # MODIFIED: Use context.event_id and context.timestamp for Gen1
        active_logger.info(f"Triggered by messageId: {context.event_id} at {context.timestamp}")
        active_logger.info(f"Decoded Pub/Sub message: {pubsub_message}")

        customer = pubsub_message.get("customer")
        project_config_name = pubsub_message.get("project")
        dataset_id = pubsub_message.get("dataset_id")
        dataset_type = pubsub_message.get("dataset_type", "items")
        current_offset = int(pubsub_message.get("offset", 0))

        if not all([customer, project_config_name, dataset_id]):
            active_logger.error(f"Missing required Pub/Sub fields: customer={customer}, project={project_config_name}, dataset_id={dataset_id}")
            raise ValueError("Missing required Pub/Sub fields: customer, project, dataset_id")

        customer_config = load_customer_config(customer)
        project_config = load_project_config(project_config_name)
        
        gcp_project_id = customer_config.get("gcp_project_id", os.environ.get("GCP_PROJECT"))
        if not gcp_project_id:
            active_logger.error("GCP Project ID not found in customer_config or GCP_PROJECT env var.")
            raise ValueError("GCP Project ID not configured.")

        active_logger = setup_logging(customer, project_config_name) # Re-initialize logger with customer/project context
        active_logger.info(f"Processing dataset_id: {dataset_id}, type: {dataset_type}, offset: {current_offset}. GCP Project: {gcp_project_id}")

        required_fields = project_config.get("required_fields", [])
        search_required_fields = project_config.get("search_required_fields", [])
        field_mappings = project_config.get("field_mappings", {})
        firestore_collection_name = project_config.get("firestore_collection")
        search_results_collection_name = project_config.get("search_results_collection", f"{firestore_collection_name}_search_results")
        firestore_db_id = project_config.get("firestore_database_id")
        gcs_bucket_name = project_config.get("gcs_bucket")

        db_options = {"project": gcp_project_id}
        if firestore_db_id:
            db_options["database"] = firestore_db_id
        db = firestore.Client(**db_options)
        storage_client = storage.Client(project=gcp_project_id)
        bucket = storage_client.bucket(gcs_bucket_name)
        error_collection_name = f"{firestore_collection_name}_errors"

        apify_key = get_secret(project_config.get("apify_api_key_secret", "apify-api-key"), gcp_project_id)
        apify_client = ApifyClient(apify_key)
        publisher = pubsub_v1.PublisherClient()

        active_logger.info(f"Initializing Vertex AI for project {gcp_project_id}, location europe-west1.")
        # aiplatform.init is generally not needed when using PredictionServiceClient directly with full endpoint path
        # aiplatform.init(project=gcp_project_id, location='europe-west1') 
        client_options = {"api_endpoint": "europe-west1-aiplatform.googleapis.com"}
        llm_client = PredictionServiceClient(client_options=client_options)
        
        llm_endpoint_str = f"projects/{gcp_project_id}/locations/europe-west1/publishers/google/models/{LLM_MODEL_ID}"
        active_logger.info(f"Using LLM endpoint: {llm_endpoint_str}")
        
        total_items_processed = 0
        dataset_exhausted = False
        results_summary = []
        error_batch = db.batch()
        error_operations_count = 0

        while total_items_processed < MAX_ITEMS_PER_INVOCATION:
            active_logger.info(f"Fetching Apify data: Dataset: {dataset_id}, Offset: {current_offset}, Limit: {APIFY_BATCH_SIZE}")
            items = None
            for attempt in range(MAX_API_RETRIES):
                try:
                    response_apify = apify_client.dataset(dataset_id).list_items(offset=current_offset, limit=APIFY_BATCH_SIZE)
                    items = response_apify.items # Corrected variable name
                    active_logger.debug(f"Apify API call successful on attempt {attempt + 1}")
                    break
                except Exception as e:
                    active_logger.warning(f"Apify API call failed on attempt {attempt + 1}: {str(e)}")
                    if attempt < MAX_API_RETRIES - 1:
                        time.sleep(RETRY_BACKOFF * (2 ** attempt))
                    else:
                        active_logger.error(f"Max retries reached for Apify API: {str(e)}")
            
            if not items:
                active_logger.info("No items fetched from Apify (either dataset exhausted or fetch failed after retries).")
                dataset_exhausted = True
                break

            active_logger.info(f"Fetched {len(items)} items.")
            firestore_batch = db.batch()
            items_in_firestore_batch = 0

            for item_index, item_data in enumerate(items):
                identifier = generate_identifier(item_data, field_mappings)
                try:
                    metadata = {
                        "raw_data": item_data, # Consider removing if too large and not always needed
                        "mainUrl": get_mapped_field(item_data, "mainUrl", field_mappings, llm_client, llm_endpoint_str, gcp_project_id, active_logger),
                        "scrape_date": item_data.get("extractionTimestamp", firestore.SERVER_TIMESTAMP),
                        "processed_at_extract_metadata": firestore.SERVER_TIMESTAMP,
                        "dataset_id_source": dataset_id,
                        "offset_source": current_offset + item_index
                    }

                    target_collection_name = search_results_collection_name if dataset_type == "search_results" else firestore_collection_name
                    fields_to_process = search_required_fields if dataset_type == "search_results" else required_fields

                    for field_name in fields_to_process:
                        metadata[field_name] = get_mapped_field(item_data, field_name, field_mappings, llm_client, llm_endpoint_str, gcp_project_id, active_logger)

                    llm_fields_to_enhance = [f_name for f_name in fields_to_process if metadata.get(f_name) in ["Not Available", "untitled", ""]]
                    if llm_fields_to_enhance:
                        active_logger.info(f"Attempting LLM enhancement for fields: {llm_fields_to_enhance} for identifier {identifier}")
                        # Prioritize specific content fields, then fall back
                        content_source_candidates = ["Content", "htmlContent", "text", "body"]
                        content_for_llm = ""
                        for source_key in content_source_candidates:
                            if item_data.get(source_key):
                                content_for_llm = item_data[source_key]
                                break
                        
                        prompt_llm_enhance = f"""
                        Analyze the following content and the current partially extracted metadata.
                        Suggest values for the missing metadata fields: {llm_fields_to_enhance}.
                        If a field implies a common concept (e.g., Title, Summary) try to derive it from the content.
                        If a suitable value cannot be found or derived for a field, its value should be "Not Available".

                        Content (first 1000 chars): {str(content_for_llm)[:1000]}
                        Current Metadata (relevant fields): { {f_name: metadata.get(f_name) for f_name in fields_to_process} }
                        Original item has keys: {list(item_data.keys())}

                        Return ONLY a JSON object with the suggested values for the missing fields. Example for fields "Title", "Author":
                        ```json
                        {{
                          "Title": "Suggested Title from Content",
                          "Author": "Not Available"
                        }}
                        ```
                        """
                        try:
                            instances_llm_enhance = [{"content": prompt_llm_enhance}] # For Gemini
                            parameters_llm_enhance = {"temperature": 0.5, "maxOutputTokens": 512, "topP": 0.8, "topK": 40}
                            
                            active_logger.debug(f"Sending to LLM for enhancement. Endpoint: {llm_endpoint_str}. Prompt: {prompt_llm_enhance[:200]}...")
                            response_llm_enhance = llm_client.predict(endpoint=llm_endpoint_str, instances=instances_llm_enhance, parameters=parameters_llm_enhance)
                            
                            if response_llm_enhance.predictions:
                                prediction_text_enhance = response_llm_enhance.predictions[0].get('content')
                                if not prediction_text_enhance: # Fallback
                                    parts_enhance = response_llm_enhance.predictions[0].get('parts')
                                    if parts_enhance and isinstance(parts_enhance, list) and parts_enhance[0].get('text'):
                                        prediction_text_enhance = parts_enhance[0]['text']

                                if prediction_text_enhance:
                                    active_logger.debug(f"LLM enhancement raw response for {identifier}: {prediction_text_enhance[:200]}")
                                    llm_suggestions_enhance = None
                                    match_enhance = re.search(r"```json\s*([\s\S]*?)\s*```", prediction_text_enhance, re.MULTILINE | re.DOTALL)
                                    if match_enhance:
                                        try:
                                            llm_suggestions_enhance = json.loads(match_enhance.group(1))
                                        except json.JSONDecodeError as e_json:
                                            active_logger.warning(f"LLM enhancement: Failed to parse JSON from markdown for {identifier}: {e_json}. Raw: {match_enhance.group(1)}")
                                    else: # Try parsing directly
                                        try:
                                            llm_suggestions_enhance = json.loads(prediction_text_enhance)
                                        except json.JSONDecodeError as e_json:
                                            active_logger.warning(f"LLM enhancement: Prediction for {identifier} not in markdown and not valid JSON: {e_json}. Raw: {prediction_text_enhance[:200]}")

                                    if llm_suggestions_enhance:
                                        for field_to_enhance in llm_fields_to_enhance:
                                            if field_to_enhance in llm_suggestions_enhance and llm_suggestions_enhance[field_to_enhance] not in ["Not Available", "untitled", ""]:
                                                metadata[field_to_enhance] = llm_suggestions_enhance[field_to_enhance]
                                                active_logger.info(f"LLM enhancement successful for field '{field_to_enhance}' for {identifier}. New value: '{str(metadata[field_to_enhance])[:100]}'")
                                            else:
                                                active_logger.info(f"LLM enhancement did not provide a new value for '{field_to_enhance}' for {identifier} (suggested: {llm_suggestions_enhance.get(field_to_enhance)}).")
                                    else:
                                         active_logger.warning(f"LLM enhancement could not parse suggestions for {identifier}. Response: {prediction_text_enhance[:200]}")
                                else:
                                    active_logger.warning(f"LLM enhancement prediction empty for {identifier}. Full prediction: {response_llm_enhance.predictions[0]}")
                            else:
                                active_logger.warning(f"LLM enhancement returned no predictions for {identifier}.")
                        except Exception as e_llm:
                            active_logger.warning(f"LLM enhancement API call failed for {identifier}: {str(e_llm)}")

                    # Remove raw_data before size check if it's too large or not strictly needed in Firestore
                    if "raw_data" in metadata and len(json.dumps(metadata["raw_data"])) > 500_000: # Heuristic
                        active_logger.info(f"Potentially large 'raw_data' for {identifier}, removing before Firestore size check.")
                        del metadata["raw_data"] # Or store it to GCS and link

                    serialized_metadata_str = json.dumps(metadata, default=str)
                    serialized_size = len(serialized_metadata_str.encode('utf-8'))

                    if serialized_size > 1_000_000: # Firestore document limit is 1 MiB
                        active_logger.warning(f"Document {identifier} (after potential LLM mapping) exceeds Firestore size limit ({serialized_size} bytes)")
                        # Store original full item_data to GCS
                        gcs_path_original_item = f"oversized_original_items/{project_config_name}/{dataset_id}/{identifier}.json.gz"
                        compress_and_upload(json.dumps(item_data, default=str), gcs_bucket_name, gcs_path_original_item)
                        
                        # Create a minimal metadata record for Firestore
                        final_metadata_for_oversized = {
                            "identifier": identifier,
                            "mainUrl": metadata.get("mainUrl", "Not Available"),
                            "scrape_date": metadata.get("scrape_date"),
                            "processed_at_extract_metadata": firestore.SERVER_TIMESTAMP,
                            "dataset_id_source": dataset_id,
                            "offset_source": current_offset + item_index,
                            "full_item_gcs_path": f"gs://{gcs_bucket_name}/{gcs_path_original_item}",
                            "status_notice": "Full item data stored in GCS due to size. Most fields are in GCS."
                        }
                        # Add a few key, small fields to the Firestore record if they exist and are small
                        for key_field in ["Title", "Law-ID", "URL", "Document-Type"]: 
                            if key_field in metadata and isinstance(metadata[key_field], str) and len(metadata[key_field]) < 200:
                                final_metadata_for_oversized[key_field] = metadata[key_field]
                        
                        doc_ref = db.collection(target_collection_name).document(identifier)
                        firestore_batch.set(doc_ref, final_metadata_for_oversized)
                        items_in_firestore_batch +=1

                        error_batch.set(db.collection(error_collection_name).document(identifier), {
                            "identifier": identifier,
                            "error": f"Document data for Firestore exceeds 1MB ({serialized_size} bytes), original item moved to GCS. Minimal metadata record created.",
                            "stage": "extract_metadata_oversized",
                            "retry_count": 0,
                            "original_item_gcs_path": f"gs://{gcs_bucket_name}/{gcs_path_original_item}",
                            "timestamp": firestore.SERVER_TIMESTAMP
                        })
                        error_operations_count += 1
                        results_summary.append({"identifier": identifier, "status": "Success (Oversized item, full data in GCS, minimal metadata in Firestore)"})
                        continue # Move to next item

                    # If not oversized, store the (potentially LLM enhanced) metadata
                    doc_ref = db.collection(target_collection_name).document(identifier)
                    firestore_batch.set(doc_ref, metadata)
                    items_in_firestore_batch += 1
                    results_summary.append({"identifier": identifier, "url": metadata.get("URL", metadata.get("mainUrl", "N/A")), "status": "Queued for Firestore"})
                
                except Exception as e_item_proc:
                    active_logger.error(f"Critical error processing item {identifier}: {str(e_item_proc)}", exc_info=True)
                    error_batch.set(db.collection(error_collection_name).document(identifier), {
                        "identifier": identifier,
                        "error": str(e_item_proc),
                        "stage": "extract_metadata_item_error",
                        "retry_count": 0,
                        "original_item": item_data, # Be cautious with large items here too
                        "timestamp": firestore.SERVER_TIMESTAMP
                    })
                    error_operations_count += 1
                    results_summary.append({"identifier": identifier, "status": f"Flagged for retry: {str(e_item_proc)}"})
                    # Publish to retry topic
                    publisher.publish(publisher.topic_path(gcp_project_id, RETRY_TOPIC_NAME), json.dumps({
                        "customer": customer, "project": project_config_name, "dataset_id": dataset_id,
                        "dataset_type": dataset_type, "identifier": identifier, "stage": "extract_metadata",
                        "item_data_snapshot": {k: str(v)[:200] for k, v in item_data.items()}, # Snapshot for retry context
                        "retry_count": 0 
                    }).encode('utf-8'))
                    continue # Move to next item

            if items_in_firestore_batch > 0:
                try:
                    active_logger.info(f"Attempting to commit {items_in_firestore_batch} documents to Firestore.")
                    items_attempted_ids_in_batch = [res["identifier"] for res in results_summary if res["status"] == "Queued for Firestore"][-items_in_firestore_batch:]
                    firestore_batch.commit()
                    active_logger.info(f"Committed {items_in_firestore_batch} documents to Firestore.")
                    for res_id in items_attempted_ids_in_batch:
                        for r_summary in results_summary:
                            if r_summary["identifier"] == res_id and r_summary["status"] == "Queued for Firestore":
                                r_summary["status"] = "Success (Firestore)"
                                break
                except Exception as e_commit:
                    active_logger.error(f"Error committing Firestore batch: {str(e_commit)}", exc_info=True)
                    for res_id in items_attempted_ids_in_batch:
                        for r_summary in results_summary:
                            if r_summary["identifier"] == res_id and r_summary["status"] == "Queued for Firestore":
                                r_summary["status"] = f"Error (Firestore commit failed): {str(e_commit)}"
                                active_logger.warning(f"Item {res_id} failed in batch commit. Sending to retry queue.")
                                error_batch.set(db.collection(error_collection_name).document(res_id), {
                                    "identifier": res_id,
                                    "error": f"Firestore batch commit failed: {str(e_commit)}. Original item needs re-queuing.",
                                    "stage": "extract_metadata_batch_commit_error",
                                    "retry_count": 0,
                                    "timestamp": firestore.SERVER_TIMESTAMP
                                })
                                error_operations_count += 1
                                publisher.publish(publisher.topic_path(gcp_project_id, RETRY_TOPIC_NAME), json.dumps({
                                    "customer": customer, "project": project_config_name, "dataset_id": dataset_id,
                                    "dataset_type": dataset_type, "identifier": res_id, "stage": "extract_metadata",
                                    "retry_count": 0
                                }).encode('utf-8'))
                                break # Found and processed this summary item

            if error_operations_count > 0:
                try:
                    active_logger.info(f"Attempting to commit {error_operations_count} error documents.")
                    error_batch.commit() # Commit the current error batch
                    active_logger.info(f"Committed {error_operations_count} error documents.")
                except Exception as e_err_commit:
                    active_logger.error(f"CRITICAL: Failed to commit error batch to Firestore: {str(e_err_commit)}", exc_info=True)
                error_operations_count = 0 # Reset for next potential batch of errors
                error_batch = db.batch() # Start a new error batch

            total_items_processed += len(items)
            current_offset += len(items)

            if len(items) < APIFY_BATCH_SIZE:
                active_logger.info(f"Fetched {len(items)} items, which is less than APIFY_BATCH_SIZE ({APIFY_BATCH_SIZE}). Assuming end of dataset.")
                dataset_exhausted = True
                break
        
        # Final commit of any pending errors if loop exited for other reasons
        if error_operations_count > 0:
            try:
                active_logger.info(f"Committing final {error_operations_count} pending error documents.")
                error_batch.commit()
                active_logger.info(f"Committed final {error_operations_count} error documents.")
            except Exception as e_final_err_commit:
                 active_logger.error(f"CRITICAL: Failed to commit final error batch: {str(e_final_err_commit)}", exc_info=True)

        active_logger.info(f"Loop finished. Processed {total_items_processed} items in this invocation. Current offset for next potential run: {current_offset}. Dataset exhausted: {dataset_exhausted}")
        
        successful_commits_in_summary = sum(1 for r in results_summary if "Success (Firestore)" in r["status"] or "Success (Oversized item" in r["status"]) # Broadened "Oversized" check
        active_logger.info(f"Summary for this invocation: {len(results_summary)} items attempted to map/process, {successful_commits_in_summary} successfully recorded in Firestore (or GCS for oversized).")

        if not dataset_exhausted and total_items_processed >= MAX_ITEMS_PER_INVOCATION: # Check if it was MAX_ITEMS that stopped it
            next_pubsub_message = {
                "customer": customer, "project": project_config_name, "dataset_id": dataset_id,
                "dataset_type": dataset_type, "offset": current_offset
            }
            topic_path_self = publisher.topic_path(gcp_project_id, SELF_TRIGGER_TOPIC_NAME)
            active_logger.info(f"Max items per invocation ({MAX_ITEMS_PER_INVOCATION}) reached. Publishing re-trigger to '{SELF_TRIGGER_TOPIC_NAME}' with offset {current_offset}: {next_pubsub_message}")
            future = publisher.publish(topic_path_self, json.dumps(next_pubsub_message).encode('utf-8'))
            try:
                message_id = future.result(timeout=30)
                active_logger.info(f"Published re-trigger message_id: {message_id}")
            except TimeoutError: # google.cloud.pubsub_v1.futures.TimeoutError
                active_logger.error("Publishing re-trigger timed out.")
                raise # Re-raise to indicate function didn't complete all tasks
        elif dataset_exhausted:
            active_logger.info(f"Dataset {dataset_id} fully processed or no more items to fetch. Triggering '{NEXT_STEP_TOPIC_NAME}'.")
            next_step_message = {
                "customer": customer, "project": project_config_name, "dataset_id": dataset_id,
                "dataset_type": dataset_type,
                "date": pubsub_message.get("date", datetime.now().strftime('%Y%m%d'))
            }
            topic_path_next = publisher.topic_path(gcp_project_id, NEXT_STEP_TOPIC_NAME)
            active_logger.info(f"Publishing to '{NEXT_STEP_TOPIC_NAME}': {next_step_message}")
            future = publisher.publish(topic_path_next, json.dumps(next_step_message).encode('utf-8'))
            try:
                message_id = future.result(timeout=30)
                active_logger.info(f"Published next-step message_id: {message_id}")
            except TimeoutError: # google.cloud.pubsub_v1.futures.TimeoutError
                active_logger.error(f"Publishing to '{NEXT_STEP_TOPIC_NAME}' timed out.")
                raise
        else: # Loop ended for other reasons, e.g. MAX_ITEMS_PER_INVOCATION not hit but dataset not marked exhausted
            active_logger.warning(f"Loop ended. Total processed: {total_items_processed}. MAX_ITEMS_PER_INVOCATION: {MAX_ITEMS_PER_INVOCATION}. Dataset exhausted: {dataset_exhausted}. No re-trigger or next step initiated under current conditions.")

        # Log metadata about the first batch run if it's the beginning of processing
        if pubsub_message.get("offset", 0) == 0 and total_items_processed > 0 :
            db.collection(f"{firestore_collection_name}_metadata").document(dataset_id).set({
                "initial_invocation_items_processed": total_items_processed,
                "initial_invocation_successful_commits": successful_commits_in_summary,
                "dataset_type": dataset_type,
                "date_triggered": pubsub_message.get("date", datetime.now().strftime('%Y%m%d')),
                "first_batch_timestamp": firestore.SERVER_TIMESTAMP
            }, merge=True)

        return {"status": "success", "items_processed_this_invocation": total_items_processed, "successful_commits_this_invocation": successful_commits_in_summary, "final_offset": current_offset}

    except Exception as e_main:
        active_logger.error(f"Critical error in extract_metadata: {str(e_main)}", exc_info=True)
        # Depending on Cloud Function retry policy, this might be re-invoked.
        # If not, ensure some external monitoring catches this failure.
        raise # Re-raise to mark the function execution as failed.