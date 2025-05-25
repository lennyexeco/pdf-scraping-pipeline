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
from src.common.helpers import find_url_in_item, get_mapped_field
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
        identifier_html = pubsub_message.get("identifier")
        main_url_html = pubsub_message.get("mainUrl")
        fixed_html_content = pubsub_message.get("fixed_html_content")
        images_fixed_count_html = pubsub_message.get("images_fixed_count", 0)
        computed_fields = {k: v for k, v in pubsub_message.items() if k in project_config.get('required_fields', [])}
        triggering_run_id_augment = pubsub_message.get("dataset_id")
        current_offset_augment = int(pubsub_message.get("offset", 0))
        apify_search_override = pubsub_message.get("apify_search_results_dataset_id_override")
        date_str = pubsub_message.get("date", datetime.now().strftime('%Y%m%d'))

        if not customer or not project_config_name:
            active_logger.error(f"Missing customer or project in Pub/Sub message.")
            raise ValueError("Missing customer or project.")

        customer_config = load_customer_config(customer)
        project_config = load_project_config(project_config_name)
        gcp_project_id = customer_config.get("gcp_project_id", os.environ.get("GCP_PROJECT"))
        if not gcp_project_id:
            raise ValueError("GCP Project ID not configured.")

        active_logger = setup_logging(customer, project_config_name)

        field_mappings = project_config.get("field_mappings", {})
        required_fields = project_config.get("required_fields", [])
        firestore_collection_name = project_config.get("firestore_collection")
        firestore_db_id = project_config.get("firestore_database_id", "(default)")
        error_collection_name = f"{firestore_collection_name}_errors"
        gcs_bucket_name = project_config.get("gcs_bucket")

        db_options = {"project": gcp_project_id}
        if firestore_db_id != "(default)":
            db_options["database"] = firestore_db_id
        db = firestore.Client(**db_options)
        publisher = pubsub_v1.PublisherClient()
        storage_client = storage.Client(project=gcp_project_id)

        # SCENARIO 1: Message from fix_image_urls with fixed_html_content
        if identifier_html and main_url_html and fixed_html_content is not None:
            active_logger.info(f"Processing passed HTML content for identifier: {identifier_html}")
            fixed_html_content = validate_html_content(fixed_html_content, active_logger)
            if not fixed_html_content:
                active_logger.error(f"Invalid HTML content for {identifier_html}.")
                return {"status": "error", "message": "Invalid HTML content."}

            doc_ref = db.collection(firestore_collection_name).document(identifier_html)
            doc = doc_ref.get()

            if not doc.exists:
                active_logger.error(f"Firestore document {identifier_html} not found when trying to store fixed_html_content.")
                return {"status": "error", "message": "Document not found for storing HTML content."}

            doc_dict_existing = doc.to_dict()

            update_data_html_store = {
                "images_fixed_count": images_fixed_count_html,
                "processing_stage": "html_content_finalized_in_fs",
                "last_updated": firestore.SERVER_TIMESTAMP,
                "fixed_html_content_in_fs": fixed_html_content
            }
            update_data_html_store.update(computed_fields)

            for attempt in range(MAX_FIRESTORE_RETRIES):
                try:
                    doc_ref.update(update_data_html_store)
                    active_logger.info(f"Updated Firestore for {identifier_html} with fixed HTML content details.")
                    break
                except Exception as e_update:
                    active_logger.warning(f"Firestore update failed on attempt {attempt + 1}: {str(e_update)}")
                    if attempt < MAX_FIRESTORE_RETRIES - 1:
                        time.sleep(RETRY_BACKOFF_AUGMENT * (2 ** attempt))
                    else:
                        active_logger.error(f"Max retries reached for Firestore update: {str(e_update)}")
                        raise

            next_step_payload = {
                "customer": customer,
                "project": project_config_name,
                "identifier": identifier_html,
                "mainUrl": main_url_html,
                "gcs_path": doc_dict_existing.get("html_path"),
                "date": date_str,
                "dataset_type": "items",
                "dataset_id": pubsub_message.get("apify_dataset_id_source"),
                "apify_run_id_trigger": pubsub_message.get("apify_run_id_trigger")
            }
            publisher.publish(
                publisher.topic_path(gcp_project_id, NEXT_STEP_GENERATE_XML_TOPIC_NAME),
                json.dumps(next_step_payload, default=str).encode('utf-8')
            )
            active_logger.info(f"Published to '{NEXT_STEP_GENERATE_XML_TOPIC_NAME}' for {identifier_html} after storing HTML details.")
            return {"status": "success", "identifier": identifier_html, "message": "Processed fixed HTML content trigger."}

        # SCENARIO 2: Message for Augmentation from secondary dataset
        elif triggering_run_id_augment:
            active_logger.info(f"Processing augmentation data. Triggering Run ID for Augmentation: {triggering_run_id_augment}, Offset: {current_offset_augment}.")
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

                possible_urls = [
                    item_data_augment.get('main_url') or item_data_augment.get('mainUrl'),
                    item_data_augment.get('url'),
                    item_data_augment.get('htmlUrl')
                ]
                identifiers = []
                for url in possible_urls:
                    if url:
                        identifiers.append(generate_url_hash(url))
                doc_ref_aug = None
                doc_dict_aug_existing = None
                identifier_aug = None
                for ident in identifiers:
                    doc_ref_aug = db.collection(firestore_collection_name).document(ident)
                    doc_aug = doc_ref_aug.get()
                    if doc_aug.exists:
                        doc_dict_aug_existing = doc_aug.to_dict()
                        identifier_aug = ident
                        break
                if not doc_aug.exists:
                    active_logger.warning(f"No Firestore doc found for URLs {possible_urls} (identifiers: {identifiers}). Skipping augmentation.")
                    results_summary_aug.append({"id": identifiers, "url": item_url_augment, "status": "Skipped: Base doc not found"})
                    continue

                update_data_aug = {}
                new_fields_added_aug = 0

                # Apply field mappings for augmentation
                for fs_target_field in required_fields:
                    mapping_details = field_mappings.get(fs_target_field, {})
                    if not isinstance(mapping_details, dict):
                        continue
                    source_definition = mapping_details.get("source")
                    mapping_type = mapping_details.get("type")

                    if mapping_type == "direct" and source_definition:
                        source_options = [s.strip() for s in source_definition.split("||")]
                        value_to_store = None
                        source_found = None
                        for apify_source_field in source_options:
                            if apify_source_field in item_data_augment:
                                value_to_store = item_data_augment[apify_source_field]
                                source_found = apify_source_field
                                break

                        if source_found and value_to_store is not None:
                            if fs_target_field not in doc_dict_aug_existing or \
                               doc_dict_aug_existing.get(fs_target_field) != value_to_store:
                                update_data_aug[fs_target_field] = value_to_store
                                new_fields_added_aug += 1
                                active_logger.debug(f"Augmenting {identifier_aug}: Set '{fs_target_field}' from Apify field '{source_found}' to '{value_to_store}'.")

                # PDF Handling
                pdf_field = 'pdfLink'
                pdf_url_from_data = update_data_aug.get(pdf_field) or item_data_augment.get('pdfLink') or item_data_augment.get('pdf_link')
                if pdf_url_from_data and isinstance(pdf_url_from_data, str):
                    active_logger.info(f"Processing {pdf_field} for {identifier_aug}: {pdf_url_from_data}")
                    pdf_gcs_path = None
                    for attempt_pdf in range(MAX_PDF_RETRIES):
                        try:
                            response_pdf = requests.get(pdf_url_from_data, timeout=20)
                            response_pdf.raise_for_status()
                            pdf_content = response_pdf.content
                            pdf_filename_gcs = f"pdfs/{project_config_name}/{date_str}/{identifier_aug}.pdf"
                            pdf_blob = storage_client.bucket(gcs_bucket_name).blob(pdf_filename_gcs)
                            pdf_blob.upload_from_string(pdf_content, content_type="application/pdf")
                            pdf_gcs_path = f"gs://{gcs_bucket_name}/{pdf_filename_gcs}"
                            update_data_aug[pdf_field] = pdf_url_from_data
                            update_data_aug["pdf_gcs_path"] = pdf_gcs_path
                            new_fields_added_aug += 1
                            active_logger.info(f"Stored PDF for {identifier_aug} at {pdf_gcs_path} (attempt {attempt_pdf+1}).")
                            break
                        except Exception as e_pdf:
                            active_logger.warning(f"PDF attempt {attempt_pdf+1} for {identifier_aug} failed: {e_pdf}")
                            if attempt_pdf < MAX_PDF_RETRIES - 1:
                                time.sleep(RETRY_BACKOFF_AUGMENT * (2**attempt_pdf))
                            else:
                                active_logger.error(f"Max PDF retries for {identifier_aug}. Error: {e_pdf}")
                                break

                if new_fields_added_aug > 0:
                    update_data_aug["last_updated"] = firestore.SERVER_TIMESTAMP
                    update_data_aug["processing_stage"] = "augmented_from_search_data"
                    update_data_aug["augmented_by_search_at"] = item_data_augment.get("extractionTimestamp", datetime.now(timezone.utc))
                    update_data_aug["apify_search_data_source_dataset"] = apify_augmentation_dataset_id
                    firestore_batch_aug.update(doc_ref_aug, update_data_aug)
                    items_in_firestore_batch_aug += 1
                    results_summary_aug.append({"id": identifier_aug, "status": f"Merged {new_fields_added_aug} fields."})
                else:
                    results_summary_aug.append({"id": identifier_aug, "status": "No new fields to augment."})

            if items_in_firestore_batch_aug > 0:
                active_logger.info(f"Committing {items_in_firestore_batch_aug} augmentation updates.")
                for attempt in range(MAX_FIRESTORE_RETRIES):
                    try:
                        firestore_batch_aug.commit()
                        break
                    except Exception as e_commit:
                        active_logger.warning(f"Firestore batch commit failed on attempt {attempt + 1}: {str(e_commit)}")
                        if attempt < MAX_FIRESTORE_RETRIES - 1:
                            time.sleep(RETRY_BACKOFF_AUGMENT * (2 ** attempt))
                        else:
                            active_logger.error(f"Max retries reached for Firestore batch commit: {str(e_commit)}")
                            raise
            if error_operations_count_aug > 0:
                active_logger.info(f"Committing {error_operations_count_aug} error records.")
                for attempt in range(MAX_FIRESTORE_RETRIES):
                    try:
                        error_batch_aug.commit()
                        break
                    except Exception as e_commit:
                        active_logger.warning(f"Error batch commit failed on attempt {attempt + 1}: {str(e_commit)}")
                        if attempt < MAX_FIRESTORE_RETRIES - 1:
                            time.sleep(RETRY_BACKOFF_AUGMENT * (2 ** attempt))
                        else:
                            active_logger.error(f"Max retries reached for error batch commit: {str(e_commit)}")
                            raise

            # Pagination for augmentation dataset
            if not TEST_PROCESS_LIMIT_AUGMENT and not dataset_exhausted_aug and \
               (total_items_processed_aug > 0 or (len(augment_items) == APIFY_BATCH_SIZE_AUGMENT)):
                next_offset_augment = current_offset_augment + APIFY_BATCH_SIZE_AUGMENT
                retrigger_msg_aug = {
                    "customer": customer,
                    "project": project_config_name,
                    "dataset_id": triggering_run_id_augment,
                    "apify_search_results_dataset_id_override": apify_augmentation_dataset_id,
                    "offset": next_offset_augment,
                    "date": date_str
                }
                active_logger.info(f"Re-triggering augmentation to '{SELF_TRIGGER_AUGMENT_TOPIC_NAME}' with offset {next_offset_augment}.")
                publisher.publish(
                    publisher.topic_path(gcp_project_id, SELF_TRIGGER_AUGMENT_TOPIC_NAME),
                    json.dumps(retrigger_msg_aug, default=str).encode('utf-8')
                )
            else:
                active_logger.info(f"Augmentation for {apify_augmentation_dataset_id} complete for this branch or test limit.")

            return {"status": "success", "items_processed_augmentation": total_items_processed_aug, "results_summary": results_summary_aug}

        else:
            active_logger.warning("store_html triggered without 'fixed_html_content' or 'dataset_id' for augmentation. No action taken.")
            return {"status": "no_action", "message": "Triggered without actionable data for HTML storage or augmentation."}

    except Exception as e_main:
        active_logger.error(f"Critical error in store_html: {str(e_main)}", exc_info=True)
        gcp_project_id_fallback = os.environ.get("GCP_PROJECT")
        if 'pubsub_message' in locals() and pubsub_message.get("customer") and pubsub_message.get("project"):
            current_gcp_project_id = gcp_project_id if 'gcp_project_id' in locals() and gcp_project_id else gcp_project_id_fallback
            if current_gcp_project_id:
                try:
                    retry_payload = {
                        "customer": pubsub_message.get("customer"),
                        "project": pubsub_message.get("project_config_name", pubsub_message.get("project")),
                        "original_pubsub_message": pubsub_message,
                        "error_message": str(e_main),
                        "stage": "store_html_critical",
                        "retry_count": pubsub_message.get("retry_count", 0) + 1,
                        "identifier_html": pubsub_message.get("identifier"),
                        "doc_data": doc_dict_existing if 'doc_dict_existing' in locals() else None
                    }
                    if publisher is None:
                        publisher = pubsub_v1.PublisherClient()
                    publisher.publish(
                        publisher.topic_path(current_gcp_project_id, RETRY_TOPIC_NAME),
                        json.dumps(retry_payload, default=str).encode('utf-8')
                    )
                    active_logger.info("Published critical error from store_html to retry topic.")
                except Exception as e_retry_pub:
                    active_logger.error(f"Failed to publish critical error from store_html to retry topic: {str(e_retry_pub)}")
            else:
                active_logger.error("GCP Project ID for error reporting not found in store_html.")
        raise