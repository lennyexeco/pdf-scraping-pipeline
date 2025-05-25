import json
import base64
import logging
import os
import time
import re
from google.cloud import firestore, pubsub_v1, storage
from apify_client import ApifyClient
from src.common.utils import generate_url_hash, setup_logging
from src.common.config import load_customer_config, load_project_config, get_secret
from src.common.helpers import find_url_in_item, get_mapped_field, generate_law_id, analyze_error_with_vertex_ai
from vertexai.generative_models import GenerativeModel, Part
import vertexai
from datetime import datetime, timezone
import functions_framework

logger = logging.getLogger(__name__)

MAX_ITEMS_PER_INVOCATION = int(os.environ.get("MAX_ITEMS_PER_INVOCATION", 250))
APIFY_BATCH_SIZE = int(os.environ.get("APIFY_BATCH_SIZE", 50))
SELF_TRIGGER_TOPIC_NAME = "process-data"
NEXT_STEP_FIX_IMAGES_TOPIC_NAME = "fix-image-urls"
RETRY_TOPIC_NAME = "retry-pipeline"
MAX_API_RETRIES = 3
MAX_FIRESTORE_RETRIES = 3
RETRY_BACKOFF = 5
MAX_DOCUMENT_SIZE = 1_000_000
LLM_MODEL_ID_FOR_HTML_DETECTION = "gemini-2.0-flash-lite-001"

TEST_PROCESS_LIMIT_ENV = os.environ.get("TEST_PROCESS_LIMIT")
TEST_PROCESS_LIMIT = None
if TEST_PROCESS_LIMIT_ENV:
    try:
        TEST_PROCESS_LIMIT = int(TEST_PROCESS_LIMIT_ENV)
        if TEST_PROCESS_LIMIT <= 0:
            TEST_PROCESS_LIMIT = None
            logger.info("TEST_PROCESS_LIMIT was not positive, disabling test limit.")
        else:
            logger.info(f"TEST_PROCESS_LIMIT is active: {TEST_PROCESS_LIMIT}")
    except ValueError:
        TEST_PROCESS_LIMIT = None
        logger.warning(f"Invalid TEST_PROCESS_LIMIT value: '{TEST_PROCESS_LIMIT_ENV}'.")

def validate_html_content(content, logger_instance):
    """Validate and normalize HTML content encoding."""
    if not content:
        return None
    try:
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
        content.encode('utf-8')
        return content
    except Exception as e:
        logger_instance.error(f"Failed to validate HTML content: {str(e)}")
        return None

def get_html_content_from_item(item_data, project_config, gcp_project_id, active_logger):
    html_content = None
    html_field_found = None
    html_fields_to_check = project_config.get('html_content_fields',
                                             ['htmlContent', 'html', 'body', 'text', 'content'])

    for field_name in html_fields_to_check:
        value = item_data.get(field_name)
        if isinstance(value, str) and value.strip():
            if '<html' in value.lower() or ('<body' in value.lower() and ('<p' in value.lower() or '<div' in value.lower())):
                html_content = validate_html_content(value, active_logger)
                if html_content:
                    html_field_found = field_name
                    active_logger.info(f"Found HTML content in direct field '{html_field_found}'.")
                    return html_content, html_field_found

    if not html_content and project_config.get("enable_llm_html_field_detection", False):
        active_logger.info("HTML content not found in standard fields. Attempting LLM inference for HTML field name.")
        try:
            vertexai.init(project=gcp_project_id, location=project_config.get("vertex_ai_location", "europe-west1"))
            llm_model = GenerativeModel(LLM_MODEL_ID_FOR_HTML_DETECTION)
            item_data_sample = {
                k: (str(v)[:100] + '...' if isinstance(v, str) and len(v) > 100 else v)
                for k, v in item_data.items() if k not in html_fields_to_check
            }
            if not item_data_sample:
                item_data_sample = {"info": "Only previously checked fields were present or item is small."}

            prompt = f"""
            Given the Apify item data structure (keys and sample values):
            {json.dumps(item_data_sample, indent=2)}
            Identify the field name that MOST LIKELY contains the primary HTML content of the webpage.
            Return a JSON object with a single key "html_field_candidate": Example: {{"html_field_candidate": "nameOfTheHtmlField"}}
            If no field seems to contain HTML, return: {{"html_field_candidate": "Not Found"}}
            """
            response = llm_model.generate_content(
                [Part.from_text(prompt)],
                generation_config={"temperature": 0.2, "max_output_tokens": 100}
            )
            candidate_text = response.candidates[0].content.parts[0].text.strip()
            json_match = re.search(r"\{.*\}", candidate_text, re.DOTALL)
            if json_match:
                suggestion_json = json.loads(json_match.group(0))
                inferred_html_field = suggestion_json.get("html_field_candidate")
                if inferred_html_field and inferred_html_field != "Not Found":
                    value = item_data.get(inferred_html_field)
                    if isinstance(value, str) and value.strip():
                        html_content = validate_html_content(value, active_logger)
                        if html_content and ('<html' in html_content.lower() or '<body' in html_content.lower()):
                            html_field_found = inferred_html_field
                            active_logger.info(f"LLM inferred HTML content field: '{html_field_found}'.")
                            return html_content, html_field_found
                    active_logger.warning(f"LLM suggested field '{inferred_html_field}' but it did not contain valid HTML or was empty.")
                else:
                    active_logger.warning("LLM could not identify a candidate HTML field or returned 'Not Found'.")
            else:
                active_logger.warning(f"LLM response for HTML field inference was not valid JSON: {candidate_text}")
        except Exception as e_llm:
            active_logger.error(f"LLM HTML content field inference failed: {str(e_llm)}", exc_info=True)

    if not html_field_found:
        active_logger.warning("No HTML content field identified after checking standard fields and optional LLM inference.")
    return html_content, html_field_found

@functions_framework.cloud_event
def extract_metadata(cloud_event):
    active_logger = logger
    pubsub_message = {}
    publisher_client = None

    try:
        event_data = cloud_event.data
        if 'message' in event_data and 'data' in event_data['message']:
            pubsub_message_data = event_data['message']['data']
            message_id = event_data['message'].get('messageId', 'N/A')
            publish_time = event_data['message'].get('publishTime', 'N/A')
        else:
            pubsub_message_data = event_data.get('data')
            message_id = cloud_event.id if hasattr(cloud_event, 'id') else 'N/A'
            publish_time = cloud_event.time if hasattr(cloud_event, 'time') else 'N/A'

        if not pubsub_message_data:
            raise ValueError("No 'data' in Pub/Sub message.")

        pubsub_message = json.loads(base64.b64decode(pubsub_message_data).decode('utf-8'))
        active_logger.info(f"Triggered by messageId: {message_id} at {publish_time}")

        customer = pubsub_message.get("customer")
        project_config_name = pubsub_message.get("project")
        triggering_run_id = pubsub_message.get("dataset_id")
        current_offset = int(pubsub_message.get("offset", 0))

        if not all([customer, project_config_name, triggering_run_id]):
            active_logger.error(f"Missing required Pub/Sub fields: customer, project, dataset_id.")
            raise ValueError("Missing required Pub/Sub fields.")

        customer_config = load_customer_config(customer)
        project_config = load_customer_config(project_config_name)
        gcp_project_id = customer_config.get("gcp_project_id", os.environ.get("GCP_PROJECT"))
        if not gcp_project_id:
            active_logger.error("GCP Project ID not found.")
            raise ValueError("GCP Project ID not configured.")

        active_logger = setup_logging(customer, project_config_name)
        active_logger.info(f"Extracting metadata for HTML content. Triggering Run ID: {triggering_run_id}, Offset: {current_offset}.")

        apify_contents_dataset_id = pubsub_message.get("apify_contents_dataset_id_override") or \
                                   project_config.get("apify_contents_dataset_id") or \
                                   triggering_run_id

        if not apify_contents_dataset_id:
            active_logger.error("Apify Dataset ID for HTML content not found.")
            raise ValueError("Apify HTML Content Dataset ID not configured.")
        active_logger.info(f"Using Apify Dataset for HTML content: {apify_contents_dataset_id}")

        field_mappings = project_config.get("field_mappings", {})
        required_fields = project_config.get("required_fields", [])
        firestore_collection_name = project_config.get("firestore_collection")
        firestore_db_id = project_config.get("firestore_database_id", "(default)")
        gcs_bucket_name = project_config.get("gcs_bucket")
        gcs_temp_html_path_template = project_config.get("gcs_temp_html_path", f"temp_html/{project_config_name}/<date>")
        error_collection_name = f"{firestore_collection_name}_errors"

        missing_configs = []
        if not firestore_collection_name: missing_configs.append("firestore_collection")
        if not gcs_bucket_name: missing_configs.append("gcs_bucket")
        if not gcs_temp_html_path_template: missing_configs.append("gcs_temp_html_path")
        if missing_configs:
            error_msg = f"Missing critical configuration: {', '.join(missing_configs)}"
            active_logger.error(error_msg)
            raise ValueError(error_msg)

        db_options = {"project": gcp_project_id}
        if firestore_db_id != "(default)":
            db_options["database"] = firestore_db_id
        db = firestore.Client(**db_options)
        storage_client = storage.Client(project=gcp_project_id)
        apify_key = get_secret(project_config.get("apify_api_key_secret", "apify-api-key"), gcp_project_id)
        apify_client = ApifyClient(apify_key)
        publisher_client = pubsub_v1.PublisherClient()

        total_items_processed = 0
        dataset_exhausted = not apify_contents_dataset_id
        results_summary = []
        error_batch = db.batch()
        error_operations_count = 0
        firestore_batch = db.batch()
        items_in_firestore_batch = 0

        sequence_doc_ref = db.collection(f"{firestore_collection_name}_metadata").document("sequence")
        sequence_doc = sequence_doc_ref.get()
        sequence_number = sequence_doc.to_dict().get("last_sequence", 0) + 1 if sequence_doc.exists else 1

        content_items = []
        if apify_contents_dataset_id:
            dataset_exhausted = False
            for attempt in range(MAX_API_RETRIES):
                try:
                    content_response = apify_client.dataset(apify_contents_dataset_id).list_items(offset=current_offset, limit=APIFY_BATCH_SIZE)
                    content_items = content_response.items
                    active_logger.info(f"Fetched {len(content_items)} content items from Apify dataset {apify_contents_dataset_id} (offset {current_offset}).")
                    if len(content_items) < APIFY_BATCH_SIZE:
                        active_logger.info(f"Content dataset {apify_contents_dataset_id} exhausted.")
                        dataset_exhausted = True
                    break
                except Exception as e:
                    active_logger.warning(f"Apify API call for content dataset {apify_contents_dataset_id} failed (attempt {attempt + 1}): {e}")
                    if attempt < MAX_API_RETRIES - 1:
                        time.sleep(RETRY_BACKOFF * (2 ** attempt))
                    else:
                        active_logger.error(f"Max retries for Apify content API (dataset {apify_contents_dataset_id}): {e}")
                        dataset_exhausted = True
                        content_items = []
                        break

        items_to_process = content_items
        if TEST_PROCESS_LIMIT is not None and len(content_items) > TEST_PROCESS_LIMIT:
            active_logger.info(f"TEST_PROCESS_LIMIT: Processing first {TEST_PROCESS_LIMIT} of {len(content_items)} content items.")
            items_to_process = content_items[:TEST_PROCESS_LIMIT]

        for item_index, item_data in enumerate(items_to_process):
            if total_items_processed >= MAX_ITEMS_PER_INVOCATION:
                active_logger.info(f"Reached MAX_ITEMS_PER_INVOCATION ({MAX_ITEMS_PER_INVOCATION}).")
                dataset_exhausted = False
                break
            total_items_processed += 1

            item_url, _ = find_url_in_item(item_data, active_logger)
            if not item_url:
                temp_identifier = generate_url_hash(f"temp-no-url-content-{item_data.get('id', 'unknown')}-{datetime.now().isoformat()}")
                active_logger.warning(f"Skipping item at offset {current_offset + item_index} from {apify_contents_dataset_id} due to missing URL. Temp ID: {temp_identifier}")
                results_summary.append({"identifier": temp_identifier, "url": "N/A", "status": "Error: No URL found for content item"})
                error_doc_data = {
                    "identifier": temp_identifier, "error": "Content Item missing URL", "stage": "extract_metadata_url_check",
                    "original_item_excerpt": {k: str(v)[:200] for k, v in item_data.items()}, "timestamp": firestore.SERVER_TIMESTAMP,
                    "customer": customer, "project": project_config_name,
                    "apify_dataset_id_source": apify_contents_dataset_id, "original_offset": current_offset + item_index
                }
                error_batch.set(db.collection(error_collection_name).document(temp_identifier), error_doc_data)
                error_operations_count += 1
                continue

            identifier = generate_url_hash(item_url)
            html_content, html_content_source_field = get_html_content_from_item(item_data, project_config, gcp_project_id, active_logger)

            if not html_content:
                active_logger.warning(f"Skipping item {identifier} (URL: {item_url}) from {apify_contents_dataset_id} as no HTML content was found.")
                results_summary.append({"identifier": identifier, "url": item_url, "status": "Skipped: No HTML content found"})
                error_doc_data = {
                    "identifier": identifier, "main_url": item_url, "error": "Item skipped: No HTML content found.",
                    "stage": "extract_metadata_html_content_check", "timestamp": firestore.SERVER_TIMESTAMP,
                    "customer": customer, "project": project_config_name,
                    "apify_dataset_id_source": apify_contents_dataset_id, "original_offset": current_offset + item_index
                }
                error_batch.set(db.collection(error_collection_name).document(identifier + "_nohtml"), error_doc_data)
                error_operations_count += 1
                continue

            metadata = {
                "scrape_date": item_data.get("extractionTimestamp", datetime.now(timezone.utc)),
                "apify_dataset_id_source": apify_contents_dataset_id,
                "apify_run_id_trigger": triggering_run_id,
                "source_offset": current_offset + item_index,
                "main_url": item_url,
                "processing_stage": "metadata_extracted_html_gcs_temp",
                "last_updated": firestore.SERVER_TIMESTAMP
            }

            # Apply field mappings using get_mapped_field
            for target_fs_field in required_fields:
                value = get_mapped_field(
                    item_data,
                    target_fs_field,
                    field_mappings,
                    logger_instance=active_logger,
                    extra_context={"sequence_number": sequence_number, "project": project_config_name}
                )
                if value is not None and value != "Not Available":
                    metadata[target_fs_field] = value
                    active_logger.debug(f"Mapped field '{target_fs_field}' to value: {value}")

            # Store HTML content in GCS (uncompressed)
            html_path_gcs = None
            try:
                metadata_size_check = sum(len(str(k).encode('utf-8')) + len(str(v).encode('utf-8')) for k, v in metadata.items())
                if len(html_content.encode('utf-8')) + metadata_size_check < MAX_DOCUMENT_SIZE * 2:  # Allow larger HTML since stored in GCS
                    safe_project_id_path = "".join(c if c.isalnum() else '_' for c in project_config_name)
                    date_str_path = pubsub_message.get("date", datetime.now().strftime('%Y%m%d'))
                    final_gcs_temp_html_path = gcs_temp_html_path_template.replace("<date>", date_str_path)
                    destination_blob_name = f"{final_gcs_temp_html_path.rstrip('/')}/{identifier}_temp.html"

                    bucket = storage_client.bucket(gcs_bucket_name)
                    blob = bucket.blob(destination_blob_name)
                    for attempt in range(MAX_FIRESTORE_RETRIES):
                        try:
                            blob.upload_from_string(html_content, content_type="text/html; charset=utf-8")
                            html_path_gcs = f"gs://{gcs_bucket_name}/{destination_blob_name}"
                            metadata["html_path"] = html_path_gcs
                            metadata["htmlContent_source_field"] = html_content_source_field
                            active_logger.info(f"Stored uncompressed HTML for {identifier} to GCS: {html_path_gcs}")
                            break
                        except Exception as e_upload:
                            active_logger.warning(f"GCS upload failed for {destination_blob_name} on attempt {attempt + 1}: {str(e_upload)}")
                            if attempt < MAX_FIRESTORE_RETRIES - 1:
                                time.sleep(RETRY_BACKOFF * (2 ** attempt))
                            else:
                                raise Exception(f"Max retries reached for GCS upload: {str(e_upload)}")
                else:
                    active_logger.warning(f"HTML content for {identifier} is too large. Size: {len(html_content.encode('utf-8'))}. Skipping item.")
                    error_doc_data = {
                        "identifier": identifier, "main_url": item_url, "error": "HTML content too large for GCS processing.",
                        "stage": "extract_metadata_html_size_check", "timestamp": firestore.SERVER_TIMESTAMP,
                        "customer": customer, "project": project_config_name,
                        "apify_dataset_id_source": apify_contents_dataset_id, "original_offset": current_offset + item_index
                    }
                    error_batch.set(db.collection(error_collection_name).document(identifier + "_html_oversize"), error_doc_data)
                    error_operations_count += 1
                    results_summary.append({"identifier": identifier, "url": item_url, "status": "Skipped: HTML too large for GCS path"})
                    continue
            except Exception as e_gcs_upload:
                active_logger.error(f"Failed to upload HTML for {identifier} to GCS: {e_gcs_upload}", exc_info=True)
                error_doc_data = {
                    "identifier": identifier, "main_url": item_url, "error": f"GCS HTML upload failed: {str(e_gcs_upload)}",
                    "stage": "extract_metadata_gcs_temp_upload", "timestamp": firestore.SERVER_TIMESTAMP,
                    "customer": customer, "project": project_config_name,
                    "apify_dataset_id_source": apify_contents_dataset_id, "original_offset": current_offset + item_index
                }
                error_batch.set(db.collection(error_collection_name).document(identifier + "_gcs_temp_err"), error_doc_data)
                error_operations_count += 1
                results_summary.append({"identifier": identifier, "url": item_url, "status": "Error: GCS HTML upload failed"})
                continue

            sequence_number += 1
            doc_ref = db.collection(firestore_collection_name).document(identifier)
            firestore_batch.set(doc_ref, metadata, merge=True)
            items_in_firestore_batch += 1
            results_summary.append({"identifier": identifier, "url": item_url, "status": "Metadata with GCS HTML path queued; fix-image-urls triggered"})

            pub_payload_fix_images = {
                "customer": customer,
                "project": project_config_name,
                "identifier": identifier,
                "main_url": item_url,
                "html_path": html_path_gcs,
                "date": pubsub_message.get("date", datetime.now().strftime('%Y%m%d')),
                "apify_dataset_id_source": apify_contents_dataset_id,
                "apify_run_id_trigger": triggering_run_id
            }
            publisher_client.publish(
                publisher_client.topic_path(gcp_project_id, NEXT_STEP_FIX_IMAGES_TOPIC_NAME),
                json.dumps(pub_payload_fix_images, default=str).encode('utf-8')
            )

        if items_in_firestore_batch > 0:
            active_logger.info(f"Committing {items_in_firestore_batch} document operations to Firestore.")
            for attempt in range(MAX_FIRESTORE_RETRIES):
                try:
                    firestore_batch.commit()
                    active_logger.info(f"Successfully committed {items_in_firestore_batch} document operations.")
                    break
                except Exception as e_commit:
                    active_logger.warning(f"Firestore batch commit failed on attempt {attempt + 1}: {str(e_commit)}")
                    if attempt < MAX_FIRESTORE_RETRIES - 1:
                        time.sleep(RETRY_BACKOFF * (2 ** attempt))
                    else:
                        active_logger.error(f"Max retries reached for Firestore batch commit: {str(e_commit)}")
                        raise
            if (sequence_doc.exists and (sequence_number > (sequence_doc.to_dict().get("last_sequence", 0) + 1))) or \
               (not sequence_doc.exists and sequence_number > 1):
                for attempt in range(MAX_FIRESTORE_RETRIES):
                    try:
                        sequence_doc_ref.set({"last_sequence": sequence_number - 1}, merge=True)
                        break
                    except Exception as e_update:
                        active_logger.warning(f"Sequence update failed on attempt {attempt + 1}: {str(e_update)}")
                        if attempt < MAX_FIRESTORE_RETRIES - 1:
                            time.sleep(RETRY_BACKOFF * (2 ** attempt))
                        else:
                            active_logger.error(f"Max retries reached for sequence update: {str(e_update)}")
                            raise
        if error_operations_count > 0:
            active_logger.info(f"Committing {error_operations_count} error documents.")
            for attempt in range(MAX_FIRESTORE_RETRIES):
                try:
                    error_batch.commit()
                    break
                except Exception as e_commit:
                    active_logger.warning(f"Error batch commit failed on attempt {attempt + 1}: {str(e_commit)}")
                    if attempt < MAX_FIRESTORE_RETRIES - 1:
                        time.sleep(RETRY_BACKOFF * (2 ** attempt))
                    else:
                        active_logger.error(f"Max retries reached for error batch commit: {str(e_commit)}")
                        raise

        should_retrigger = False
        if TEST_PROCESS_LIMIT is not None:
            active_logger.info(f"TEST_PROCESS_LIMIT ({TEST_PROCESS_LIMIT}) active. No re-triggering.")
        elif not dataset_exhausted:
            if total_items_processed >= MAX_ITEMS_PER_INVOCATION or \
               (total_items_processed > 0 and len(content_items) == APIFY_BATCH_SIZE) or \
               (total_items_processed == 0 and len(content_items) > 0 and not dataset_exhausted):
                should_retrigger = True

        if should_retrigger:
            next_offset = current_offset + APIFY_BATCH_SIZE
            retrigger_message = {
                "customer": customer, "project": project_config_name,
                "dataset_id": triggering_run_id,
                "apify_contents_dataset_id_override": apify_contents_dataset_id,
                "offset": next_offset,
                "date": pubsub_message.get("date", datetime.now().strftime('%Y%m%d'))
            }
            active_logger.info(f"Re-triggering for content dataset {apify_contents_dataset_id} to '{SELF_TRIGGER_TOPIC_NAME}' with offset {next_offset}")
            publisher_client.publish(
                publisher_client.topic_path(gcp_project_id, SELF_TRIGGER_TOPIC_NAME),
                json.dumps(retrigger_message, default=str).encode('utf-8')
            )
        else:
            active_logger.info(f"Content dataset {apify_contents_dataset_id} processing complete for this branch (offset {current_offset}) or test limit applied.")

        return {"status": "success", "items_processed": total_items_processed, "results_summary": results_summary}

    except Exception as e_main:
        active_logger.error(f"Critical error in extract_metadata: {str(e_main)}", exc_info=True)
        gcp_project_id_fallback = os.environ.get("GCP_PROJECT")
        if 'pubsub_message' in locals() and pubsub_message.get("customer") and pubsub_message.get("project"):
            current_gcp_project_id = gcp_project_id if 'gcp_project_id' in locals() and gcp_project_id else gcp_project_id_fallback
            if current_gcp_project_id:
                try:
                    retry_payload = {
                        "customer": pubsub_message.get("customer"),
                        "project": pubsub_message.get("project"),
                        "original_pubsub_message": pubsub_message,
                        "error_message": str(e_main),
                        "stage": "extract_metadata_critical",
                        "retry_count": pubsub_message.get("retry_count", 0) + 1,
                        "item_data": item_data if 'item_data' in locals() else None,
                        "metadata": metadata if 'metadata' in locals() else None
                    }
                    if publisher_client is None:
                        publisher_client = pubsub_v1.PublisherClient()
                    publisher_client.publish(
                        publisher_client.topic_path(current_gcp_project_id, RETRY_TOPIC_NAME),
                        json.dumps(retry_payload, default=str).encode('utf-8')
                    )
                    active_logger.info("Published critical error to retry topic.")
                except Exception as e_retry_pub:
                    active_logger.error(f"Failed to publish critical error to retry topic: {str(e_retry_pub)}")
            else:
                active_logger.error("GCP Project ID for error reporting not found.")
        raise