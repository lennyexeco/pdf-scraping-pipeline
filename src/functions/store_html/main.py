import json
import base64
import logging
import os
import time
import re
import requests
from google.cloud import firestore, pubsub_v1, storage
from apify_client import ApifyClient
from src.common.utils import generate_url_hash, setup_logging
from src.common.config import load_customer_config, load_project_config, get_secret
from src.common.helpers import find_url_in_item, get_mapped_field, sanitize_field_name
from datetime import datetime, timezone
import functions_framework

logger = logging.getLogger(__name__)

MAX_ITEMS_PER_INVOCATION_AUGMENT = int(os.environ.get("MAX_ITEMS_PER_INVOCATION_AUGMENT", 250))
APIFY_BATCH_SIZE_AUGMENT = int(os.environ.get("APIFY_BATCH_SIZE_AUGMENT", 50))
SELF_TRIGGER_AUGMENT_TOPIC_NAME = os.environ.get("SELF_TRIGGER_AUGMENT_TOPIC_NAME", "store-html-process-secondary")
RETRY_TOPIC_NAME = "retry-pipeline"
NEXT_STEP_GENERATE_XML_TOPIC_NAME = "generate-xml"
MAX_API_RETRIES_AUGMENT = 3
MAX_PDF_RETRIES = 3
MAX_FIRESTORE_RETRIES = 3
RETRY_BACKOFF_AUGMENT = 5

TEST_PROCESS_LIMIT_AUGMENT_ENV = os.environ.get("TEST_PROCESS_LIMIT_AUGMENT")
TEST_PROCESS_LIMIT_AUGMENT = None
if TEST_PROCESS_LIMIT_AUGMENT_ENV:
    try:
        TEST_PROCESS_LIMIT_AUGMENT = int(TEST_PROCESS_LIMIT_AUGMENT_ENV)
        if TEST_PROCESS_LIMIT_AUGMENT <= 0:
            TEST_PROCESS_LIMIT_AUGMENT = None
        else:
            logger.info(f"TEST_PROCESS_LIMIT_AUGMENT is active: {TEST_PROCESS_LIMIT_AUGMENT}")
    except ValueError:
        TEST_PROCESS_LIMIT_AUGMENT = None
        logger.warning("Invalid TEST_PROCESS_LIMIT_AUGMENT value.")

@firestore.transactional
def update_sequence(transaction, sequence_doc_ref, sequence_number):
    """Update sequence number atomically."""
    snapshot = sequence_doc_ref.get(transaction=transaction)
    current_sequence = snapshot.get("last_sequence", 0) if snapshot.exists else 0
    if sequence_number > current_sequence + 1:
        transaction.set(sequence_doc_ref, {"last_sequence": sequence_number - 1}, merge=True)

@functions_framework.cloud_event
def store_html(cloud_event):
    active_logger = logger
    pubsub_message = {}
    publisher = None

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
        active_logger.info(f"store_html triggered by messageId: {message_id} at {publish_time}")

        customer = pubsub_message.get("customer")
        project_config_name = pubsub_message.get("project")
        triggering_run_id_augment = pubsub_message.get("dataset_id")
        current_offset_augment = int(pubsub_message.get("offset", 0))
        apify_search_override = pubsub_message.get("apify_search_results_dataset_id_override")
        date_str = pubsub_message.get("date", datetime.now().strftime('%Y%m%d'))
        apify_dataset_id_source = pubsub_message.get("apify_dataset_id_source")
        apify_run_id_trigger = pubsub_message.get("apify_run_id_trigger")

        if not customer or not project_config_name or not triggering_run_id_augment:
            active_logger.error(f"Missing required fields in Pub/Sub message: customer, project, or dataset_id.")
            raise ValueError("Missing required fields.")

        customer_config = load_customer_config(customer)
        project_config = load_project_config(project_config_name)
        gcp_project_id = customer_config.get("gcp_project_id", os.environ.get("GCP_PROJECT"))
        if not gcp_project_id:
            raise ValueError("GCP Project ID not configured.")

        active_logger = setup_logging(customer, project_config_name)

        field_mappings = project_config.get("field_mappings", {})
        required_fields = project_config.get("required_fields", [])
        search_required_fields = project_config.get("search_required_fields", [])
        firestore_collection_name = project_config.get("firestore_collection")
        error_collection_name = f"{firestore_collection_name}_errors"
        gcs_bucket_name = project_config.get("gcs_bucket")
        gcs_pdf_path_template = project_config.get("gcs_pdf_path", f"pdf/{project_config_name}/<date>")

        db_options = {"project": gcp_project_id}
        firestore_db_id = project_config.get("firestore_database_id", "(default)")
        if firestore_db_id != "(default)":
            db_options["database"] = firestore_db_id
        db = firestore.Client(**db_options)
        publisher = pubsub_v1.PublisherClient()
        storage_client = storage.Client(project=gcp_project_id)

        active_logger.info(f"Processing augmentation data. Triggering Run ID: {triggering_run_id_augment}, Offset: {current_offset_augment}.")
        apify_augmentation_dataset_id = apify_search_override or \
                                       project_config.get("apify_search_results_dataset_id") or \
                                       triggering_run_id_augment
        if not apify_augmentation_dataset_id:
            active_logger.error("Apify Dataset ID for augmentation not configured.")
            raise ValueError("Apify Augmentation Dataset ID not configured.")

        active_logger.info(f"Using Apify Dataset for augmentation: {apify_augmentation_dataset_id}")

        apify_key = get_secret(project_config.get("apify_api_key_secret", "apify-api-key"), gcp_project_id)
        apify_client = ApifyClient(apify_key)

        total_items_processed_aug = 0
        dataset_exhausted_aug = not apify_augmentation_dataset_id
        results_summary_aug = []
        error_batch_aug = db.batch()
        error_operations_count_aug = 0
        firestore_batch_aug = db.batch()
        items_in_firestore_batch_aug = 0

        sequence_doc_ref = db.collection(f"{firestore_collection_name}_metadata").document("sequence")
        transaction = db.transaction()
        sequence_doc = sequence_doc_ref.get()
        sequence_number = sequence_doc.to_dict().get("last_sequence", 0) + 1 if sequence_doc.exists else 1

        augment_items = []
        if apify_augmentation_dataset_id:
            dataset_exhausted_aug = False
            for attempt in range(MAX_API_RETRIES_AUGMENT):
                try:
                    response = apify_client.dataset(apify_augmentation_dataset_id).list_items(
                        offset=current_offset_augment, limit=APIFY_BATCH_SIZE_AUGMENT)
                    augment_items = response.items
                    active_logger.info(f"Fetched {len(augment_items)} augmentation items from {apify_augmentation_dataset_id} (offset {current_offset_augment}).")
                    if len(augment_items) < APIFY_BATCH_SIZE_AUGMENT:
                        dataset_exhausted_aug = True
                        active_logger.info("Augmentation dataset exhausted.")
                    break
                except Exception as e_apify_aug:
                    active_logger.warning(f"Apify API call for {apify_augmentation_dataset_id} failed (attempt {attempt+1}): {e_apify_aug}")
                    if attempt < MAX_API_RETRIES_AUGMENT - 1:
                        time.sleep(RETRY_BACKOFF_AUGMENT * (2**attempt))
                    else:
                        active_logger.error(f"Max retries for Apify API (dataset {apify_augmentation_dataset_id}): {e_apify_aug}")
                        dataset_exhausted_aug = True
                        augment_items = []
                        break

        items_to_process_augment = augment_items
        if TEST_PROCESS_LIMIT_AUGMENT is not None and len(augment_items) > TEST_PROCESS_LIMIT_AUGMENT:
            items_to_process_augment = augment_items[:TEST_PROCESS_LIMIT_AUGMENT]
            active_logger.info(f"TEST_PROCESS_LIMIT_AUGMENT: Processing first {len(items_to_process_augment)} items.")

        for item_index, item_data_augment in enumerate(items_to_process_augment):
            if total_items_processed_aug >= MAX_ITEMS_PER_INVOCATION_AUGMENT:
                dataset_exhausted_aug = False
                active_logger.info("Reached MAX_ITEMS_PER_INVOCATION_AUGMENT.")
                break
            total_items_processed_aug += 1

            active_logger.debug(f"Augmentation item data (offset {current_offset_augment + item_index}): {json.dumps(item_data_augment, default=str)}")

            item_url_augment, _ = find_url_in_item(item_data_augment, active_logger)
            if not item_url_augment:
                active_logger.warning(f"Skipping augmentation item (offset {current_offset_augment + item_index}) due to missing URL.")
                results_summary_aug.append({"id": "unknown", "url": None, "status": "Skipped: Missing URL"})
                continue

            identifier_aug = generate_url_hash(item_url_augment)
            doc_ref_aug = db.collection(firestore_collection_name).document(identifier_aug)
            doc_aug = doc_ref_aug.get()
            doc_dict_aug_existing = doc_aug.to_dict() if doc_aug.exists else {}

            update_data_aug = {
                "main_url": item_url_augment,
                "apify_search_data_source_dataset": apify_augmentation_dataset_id,
                "augmented_by_search_at": item_data_augment.get("extractionTimestamp", datetime.now(timezone.utc)),
                "processing_stage": "augmented_from_search_data",
                "last_updated": firestore.SERVER_TIMESTAMP
            }
            new_fields_added_aug = 0

            for fs_target_field in required_fields + search_required_fields:
                sanitized_field = sanitize_field_name(fs_target_field)
                # Skip Law-ID if it already exists to prevent duplicates
                if fs_target_field == "Law-ID" and doc_dict_aug_existing.get("Law-ID"):
                    active_logger.debug(f"Skipping Law-ID update for {identifier_aug} as it already exists: {doc_dict_aug_existing['Law-ID']}")
                    continue
                value = get_mapped_field(
                    item_data_augment,
                    fs_target_field,
                    field_mappings,
                    logger_instance=active_logger,
                    extra_context={"project": project_config_name, "sequence_number": sequence_number}
                )
                if value and value != "Not Available":
                    existing_value = doc_dict_aug_existing.get(fs_target_field)
                    if existing_value is None or existing_value == "Not Available" or existing_value != value:
                        update_data_aug[sanitized_field] = value
                        new_fields_added_aug += 1
                        active_logger.debug(f"Augmenting {identifier_aug}: Set '{sanitized_field}' to '{value}'.")
                        if sanitized_field != fs_target_field:
                            active_logger.info(f"Sanitized field name from '{fs_target_field}' to '{sanitized_field}' for Firestore.")
                else:
                    active_logger.warning(f"Field '{fs_target_field}' not mapped for {identifier_aug}: value is '{value}'")

            pdf_field = 'pdfLink'
            pdf_url_from_data = update_data_aug.get(pdf_field) or item_data_augment.get('pdfLink') or item_data_augment.get('pdf_link')
            if pdf_url_from_data and isinstance(pdf_url_from_data, str) and pdf_url_from_data.lower().startswith(('http://', 'https://')):
                active_logger.info(f"Processing {pdf_field} for {identifier_aug}: {pdf_url_from_data}")
                safe_project_id_path = "".join(c if c.isalnum() else '_' for c in project_config_name)
                final_gcs_pdf_path = gcs_pdf_path_template.replace("<date>", date_str)
                destination_blob_name_pdf = f"{final_gcs_pdf_path.rstrip('/')}/{identifier_aug}.pdf"

                bucket = storage_client.bucket(gcs_bucket_name)
                blob_pdf = bucket.blob(destination_blob_name_pdf)
                pdf_content = None

                for attempt in range(MAX_PDF_RETRIES):
                    try:
                        response = requests.get(pdf_url_from_data, timeout=30)
                        response.raise_for_status()
                        pdf_content = response.content
                        active_logger.info(f"Successfully downloaded PDF for {identifier_aug} from {pdf_url_from_data}")
                        break
                    except Exception as e_pdf:
                        active_logger.warning(f"PDF download failed for {identifier_aug} on attempt {attempt + 1}: {str(e_pdf)}")
                        if attempt < MAX_PDF_RETRIES - 1:
                            time.sleep(RETRY_BACKOFF_AUGMENT * (2 ** attempt))
                        else:
                            active_logger.error(f"Max retries reached for PDF download: {str(e_pdf)}")
                            error_doc_data = {
                                "identifier": identifier_aug,
                                "main_url": item_url_augment,
                                "error": f"PDF download failed: {str(e_pdf)}",
                                "stage": "store_html_pdf_download",
                                "timestamp": firestore.SERVER_TIMESTAMP,
                                "customer": customer,
                                "project": project_config_name,
                                "apify_dataset_id_source": apify_augmentation_dataset_id,
                                "original_offset": current_offset_augment + item_index
                            }
                            error_batch_aug.set(db.collection(error_collection_name).document(identifier_aug + "_pdf_err"), error_doc_data)
                            error_operations_count_aug += 1
                            break

                if pdf_content:
                    try:
                        blob_pdf.upload_from_string(pdf_content, content_type="application/pdf")
                        pdf_gcs_path = f"gs://{gcs_bucket_name}/{destination_blob_name_pdf}"
                        update_data_aug[pdf_field] = pdf_gcs_path
                        active_logger.info(f"Stored PDF for {identifier_aug} to GCS: {pdf_gcs_path}")
                        new_fields_added_aug += 1
                    except Exception as e_pdf_upload:
                        active_logger.error(f"Failed to upload PDF for {identifier_aug} to GCS: {str(e_pdf_upload)}")
                        error_doc_data = {
                            "identifier": identifier_aug,
                            "main_url": item_url_augment,
                            "error": f"PDF GCS upload failed: {str(e_pdf_upload)}",
                            "stage": "store_html_pdf_gcs_upload",
                            "timestamp": firestore.SERVER_TIMESTAMP,
                            "customer": customer,
                            "project": project_config_name,
                            "apify_dataset_id_source": apify_augmentation_dataset_id,
                            "original_offset": current_offset_augment + item_index
                        }
                        error_batch_aug.set(db.collection(error_collection_name).document(identifier_aug + "_pdf_gcs_err"), error_doc_data)
                        error_operations_count_aug += 1

            if new_fields_added_aug > 0:
                firestore_batch_aug.set(doc_ref_aug, update_data_aug, merge=True)
                items_in_firestore_batch_aug += 1
                if "Law-ID" in update_data_aug:
                    sequence_number += 1
                results_summary_aug.append({
                    "identifier": identifier_aug,
                    "url": item_url_augment,
                    "status": f"Augmented with {new_fields_added_aug} new fields."
                })

                pub_payload_generate_xml = {
                    "customer": customer,
                    "project": project_config_name,
                    "identifier": identifier_aug,
                    "main_url": item_url_augment,
                    "date": date_str,
                    "apify_dataset_id_source": apify_dataset_id_source,
                    "apify_run_id_trigger": apify_run_id_trigger
                }
                publisher.publish(
                    publisher.topic_path(gcp_project_id, NEXT_STEP_GENERATE_XML_TOPIC_NAME),
                    json.dumps(pub_payload_generate_xml, default=str).encode('utf-8')
                )
                active_logger.info(f"Triggered XML generation for {identifier_aug} via {NEXT_STEP_GENERATE_XML_TOPIC_NAME}.")

            else:
                active_logger.info(f"No new fields to augment for {identifier_aug}. Skipping Firestore update and XML generation.")
                results_summary_aug.append({
                    "identifier": identifier_aug,
                    "url": item_url_augment,
                    "status": "Skipped: No new fields to augment."
                })

        if items_in_firestore_batch_aug > 0:
            active_logger.info(f"Committing {items_in_firestore_batch_aug} Firestore operations for augmentation.")
            for attempt in range(MAX_FIRESTORE_RETRIES):
                try:
                    firestore_batch_aug.commit()
                    active_logger.info(f"Successfully committed {items_in_firestore_batch_aug} Firestore operations.")
                    break
                except Exception as e_commit:
                    active_logger.warning(f"Firestore batch commit failed on attempt {attempt + 1}: {str(e_commit)}")
                    if attempt < MAX_FIRESTORE_RETRIES - 1:
                        time.sleep(RETRY_BACKOFF_AUGMENT * (2 ** attempt))
                    else:
                        active_logger.error(f"Max retries reached for Firestore batch commit: {str(e_commit)}")
                        raise
            update_sequence(transaction, sequence_doc_ref, sequence_number)

        if error_operations_count_aug > 0:
            active_logger.info(f"Committing {error_operations_count_aug} error documents for augmentation.")
            for attempt in range(MAX_FIRESTORE_RETRIES):
                try:
                    error_batch_aug.commit()
                    active_logger.info(f"Successfully committed {error_operations_count_aug} error documents.")
                    break
                except Exception as e_commit:
                    active_logger.warning(f"Error batch commit failed on attempt {attempt + 1}: {str(e_commit)}")
                    if attempt < MAX_FIRESTORE_RETRIES - 1:
                        time.sleep(RETRY_BACKOFF_AUGMENT * (2 ** attempt))
                    else:
                        active_logger.error(f"Max retries reached for error batch commit: {str(e_commit)}")
                        raise

        should_retrigger = False
        if TEST_PROCESS_LIMIT_AUGMENT is not None:
            active_logger.info(f"TEST_PROCESS_LIMIT_AUGMENT ({TEST_PROCESS_LIMIT_AUGMENT}) active. No re-triggering.")
        elif not dataset_exhausted_aug:
            if total_items_processed_aug >= MAX_ITEMS_PER_INVOCATION_AUGMENT or \
               (total_items_processed_aug > 0 and len(augment_items) == APIFY_BATCH_SIZE_AUGMENT):
                should_retrigger = True

        if should_retrigger:
            next_offset = current_offset_augment + APIFY_BATCH_SIZE_AUGMENT
            retrigger_message = {
                "customer": customer,
                "project": project_config_name,
                "dataset_id": triggering_run_id_augment,
                "offset": next_offset,
                "date": date_str,
                "apify_search_results_dataset_id_override": apify_augmentation_dataset_id,
                "apify_dataset_id_source": apify_dataset_id_source,
                "apify_run_id_trigger": apify_run_id_trigger
            }
            active_logger.info(f"Re-triggering store_html for dataset {apify_augmentation_dataset_id} with offset {next_offset}")
            publisher.publish(
                publisher.topic_path(gcp_project_id, SELF_TRIGGER_AUGMENT_TOPIC_NAME),
                json.dumps(retrigger_message, default=str).encode('utf-8')
                )
        else:
            active_logger.info(f"Augmentation dataset {apify_augmentation_dataset_id} processing complete for offset {current_offset_augment}.")

        return {
            "status": "success",
            "items_processed": total_items_processed_aug,
            "results_summary": results_summary_aug
        }

    except Exception as e_main:
        active_logger.error(f"Critical error in store_html: {str(e_main)}", exc_info=True)
        gcp_project_id_fallback = os.environ.get("GCP_PROJECT")
        if 'pubsub_message' in locals() and pubsub_message.get("customer") and pubsub_message.get("project"):
            current_gcp_project_id = gcp_project_id if 'gcp_project_id' in locals() else gcp_project_id_fallback
            if current_gcp_project_id:
                try:
                    retry_payload = {
                        "customer": pubsub_message.get("customer"),
                        "project": pubsub_message.get("project"),
                        "original_pubsub_message": pubsub_message,
                        "error_message": str(e_main),
                        "stage": "store_html_critical",
                        "retry_count": pubsub_message.get("retry_count", 0) + 1,
                        "item_data": item_data_augment if 'item_data_augment' in locals() else None,
                        "identifier": identifier_aug if 'identifier_aug' in locals() else None
                    }
                    if publisher is None:
                        publisher = pubsub_v1.PublisherClient()
                    publisher.publish(
                        publisher.topic_path(current_gcp_project_id, RETRY_TOPIC_NAME),
                        json.dumps(retry_payload, default=str).encode('utf-8')
                    )
                    active_logger.info("Published critical error to retry topic.")
                except Exception as e_retry:
                    active_logger.error(f"Failed to publish critical error to retry topic: {str(e_retry)}")
            else:
                active_logger.error("GCP Project ID for error reporting not found.")
        raise