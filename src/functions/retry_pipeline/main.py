import json
import base64
import logging
import os
import time
import requests # Added for potential re-scraping attempts if suggested by LLM

from google.cloud import firestore, pubsub_v1, storage

from src.common.utils import generate_url_hash, setup_logging
# Updated import to include load_dynamic_site_config
from src.common.config import load_customer_config, load_project_config, get_secret, load_dynamic_site_config
from src.common.helpers import (
    generate_law_id, find_url_in_item, get_mapped_field,
    sanitize_error_message, analyze_error_with_vertex_ai,
    validate_html_content
)
from datetime import datetime, timezone
import functions_framework

logger = logging.getLogger(__name__)

# Constants for Pub/Sub topics
INGEST_CATEGORY_URLS_TOPIC_NAME = "ingest-category-urls-topic"
DISCOVER_MAIN_URLS_TOPIC_NAME = "discover-main-urls-topic"
EXTRACT_INITIAL_METADATA_TOPIC_NAME = "extract-initial-metadata-topic"
FETCH_CONTENT_TOPIC_NAME = "fetch-content-topic"
GENERATE_XML_TOPIC_NAME = "generate-xml-topic"
GENERATE_REPORTS_TOPIC_NAME = "generate-reports-topic"
RETRY_TOPIC_NAME = "retry-pipeline" # Self-reference

MAX_FIRESTORE_RETRIES = 3
RETRY_BACKOFF = 5
MAX_RETRIES = 3 # Max retries for an item in this function
MAX_DOCUMENT_SIZE = 1_000_000 # 1MB, Firestore limit


@firestore.transactional
def update_sequence_in_transaction(transaction, sequence_doc_ref, new_sequence_number):
    """
    Atomically updates the sequence number in Firestore if the new number is greater.
    """
    snapshot = sequence_doc_ref.get(transaction=transaction)
    last_sequence = 0
    if snapshot.exists:
        last_sequence = snapshot.to_dict().get("last_sequence", 0)

    if new_sequence_number > last_sequence:
        transaction.set(sequence_doc_ref, {"last_sequence": new_sequence_number}, merge=True)
        logger.info(f"Updated sequence number to {new_sequence_number}")
    elif new_sequence_number == last_sequence:
        logger.info(f"Sequence number {new_sequence_number} already set.")
    else: # new_sequence_number < last_sequence
        logger.warning(f"Attempted to set sequence number {new_sequence_number} but current is {last_sequence}. No update made.")


@functions_framework.cloud_event
def retry_pipeline(cloud_event):
    active_logger = logger
    publisher = None

    try:
        pubsub_message_data_encoded = cloud_event.data["message"]["data"]
        data = json.loads(base64.b64decode(pubsub_message_data_encoded).decode("utf-8"))
        # active_logger.info(f"Decoded retry message: {json.dumps(data, default=str)}") # Can be verbose

        customer_id = data.get("customer")
        project_id_config_name = data.get("project")
        original_input_id = data.get("original_input_id")
        identifier = data.get("identifier")
        stage = data.get("stage")
        retry_count = int(data.get("retry_count", 0))
        main_url_from_payload = data.get("main_url") or data.get("item_data_snapshot", {}).get("main_url")
        category_url_from_payload = data.get("category_url") or data.get("item_data_snapshot", {}).get("category_url")

        if not all([customer_id, project_id_config_name, identifier, stage]):
            logger.error( # Use base logger if active_logger isn't set yet
                f"Missing required fields: customer={customer_id}, project={project_id_config_name}, "
                f"identifier={identifier}, stage={stage}"
            )
            raise ValueError("Missing required fields for retry.")

        active_logger = setup_logging(customer_id, project_id_config_name)
        active_logger.info(
            f"Processing retry for identifier '{identifier}', stage '{stage}', retry count {retry_count}. Message: {json.dumps(data, default=str)}"
        )

        customer_config = load_customer_config(customer_id)
        # project_config is now loaded dynamically later, after db client is initialized
        gcp_project_id = customer_config.get("gcp_project_id", os.environ.get("GCP_PROJECT"))

        if not gcp_project_id:
            active_logger.error("GCP Project ID not found in customer config or environment.")
            raise ValueError("GCP Project ID not configured.")

        db_options = {"project": gcp_project_id}
        # Need project_config to get firestore_database_id *before* initializing db to pass it
        # So, we load static config first for this, then load dynamic config.
        static_project_config = load_project_config(project_id_config_name)
        firestore_db_id = static_project_config.get("firestore_database_id", "(default)")

        if firestore_db_id != "(default)":
            db_options["database"] = firestore_db_id
        db = firestore.Client(**db_options)

        # --- Load Dynamic/Merged Project Configuration ---
        # This now uses your new load_dynamic_site_config function
        project_config = load_dynamic_site_config(db, project_id_config_name, active_logger)
        # -------------------------------------------------

        firestore_collection_name = project_config.get("firestore_collection", static_project_config.get("firestore_collection"))
        gcs_bucket_name = project_config.get("gcs_bucket", static_project_config.get("gcs_bucket"))
        error_collection_name = f"{firestore_collection_name}_errors"

        if not firestore_collection_name or not gcs_bucket_name:
            active_logger.error("firestore_collection_name or gcs_bucket_name is missing from configuration.")
            raise ValueError("Essential configuration (firestore_collection_name, gcs_bucket_name) missing.")


        storage_client = storage.Client(project=gcp_project_id)
        # bucket = storage_client.bucket(gcs_bucket_name) # Initialize bucket object - not directly used here
        publisher = pubsub_v1.PublisherClient()
        retry_topic_path = publisher.topic_path(gcp_project_id, RETRY_TOPIC_NAME)

        error_doc_ref = db.collection(error_collection_name).document(str(identifier))
        error_doc = error_doc_ref.get()

        if not error_doc.exists:
            active_logger.error(f"No error document found for identifier '{identifier}' in '{error_collection_name}'. Cannot process retry.")
            return {"status": "error", "message": "No error document found for identifier."}

        error_data = error_doc.to_dict()
        original_item_snapshot = data.get("item_data_snapshot") or error_data.get("original_item_snapshot") or {}
        error_message_from_doc = error_data.get("error", "No error message recorded in error document.")
        current_error_message = data.get("error_message", error_message_from_doc)

        if retry_count >= MAX_RETRIES:
            active_logger.error(f"Max retries ({MAX_RETRIES}) reached for identifier '{identifier}' at stage '{stage}'. Error: {current_error_message}")
            error_doc_ref.update({
                "status": "Unresolvable_Max_Retries",
                "last_retry_attempt_timestamp": firestore.SERVER_TIMESTAMP,
                "final_error_message": current_error_message
            })
            return {"status": "failed_max_retries", "message": f"Max retries reached for {identifier} at stage {stage}."}

        main_doc_ref = db.collection(firestore_collection_name).document(str(identifier))
        main_doc = main_doc_ref.get()
        main_doc_data = main_doc.to_dict() if main_doc.exists else {}

        # LLM analysis now uses the potentially merged `project_config`
        llm_suggestion = analyze_error_with_vertex_ai(
            error_message=current_error_message,
            stage=stage,
            field_mappings=project_config.get("field_mappings", {}),
            dataset_fields=list(original_item_snapshot.keys()),
            gcp_project_id=gcp_project_id,
            logger_instance=active_logger,
            extra_context={"firestore_doc_data": main_doc_data, "project_config": project_config} # Pass merged config
        )

        adjusted_params = llm_suggestion.get("adjusted_params", {})
        attempt_re_scrape = adjusted_params.get("recheck_metadata", False) or adjusted_params.get("retry_fetch", False)
        truncate_html_content = adjusted_params.get("truncate_content", False)

        active_logger.info(
            f"LLM suggestion for '{identifier}': Retry={llm_suggestion.get('retry', False)}, "
            f"Reason='{llm_suggestion.get('reason', 'N/A')}', Category='{llm_suggestion.get('category', 'N/A')}', "
            f"Adjusted Params: {adjusted_params}"
        )

        error_doc_ref.update({
            "llm_suggestion_history": firestore.ArrayUnion([llm_suggestion]),
            "last_retry_attempt_timestamp": firestore.SERVER_TIMESTAMP,
            "retry_attempt_details": firestore.ArrayUnion([{
                "timestamp": firestore.SERVER_TIMESTAMP,
                "retry_count": retry_count + 1,
                "stage": stage,
                "llm_suggestion": llm_suggestion,
                "applied_params": adjusted_params
            }])
        })

        if not llm_suggestion.get("retry", True):
            active_logger.warning(f"LLM advised not to retry for identifier '{identifier}' at stage '{stage}'. Reason: {llm_suggestion.get('reason')}")
            error_doc_ref.update({"status": f"Unresolvable_LLM_Advised_No_Retry"})
            return {"status": "failed_llm_no_retry", "message": f"LLM advised not to retry for {identifier}."}

        next_retry_count_for_pubsub = retry_count + 1

        def publish_for_next_retry_attempt(error_msg_for_doc="Retry attempt failed", item_snapshot_for_next=None, specific_doc_data_for_next=None):
            error_doc_ref.update({
                "retry_count": next_retry_count_for_pubsub,
                "error": f"Retry attempt {retry_count + 1} for stage '{stage}' failed, queueing attempt {next_retry_count_for_pubsub}. Last error: {sanitize_error_message(error_msg_for_doc)}",
                "status": "Retrying_Queued"
            })
            retry_message_payload_dict = {
                "customer": customer_id,
                "project": project_id_config_name,
                "original_input_id": original_input_id,
                "identifier": identifier,
                "main_url": main_url_from_payload,
                "category_url": category_url_from_payload,
                "stage": stage,
                "retry_count": next_retry_count_for_pubsub,
                "item_data_snapshot": item_snapshot_for_next or original_item_snapshot,
                "error_message": error_msg_for_doc
            }
            if specific_doc_data_for_next:
                 retry_message_payload_dict["doc_data_snapshot"] = specific_doc_data_for_next

            publisher.publish(retry_topic_path, json.dumps(retry_message_payload_dict, default=str).encode('utf-8'))
            active_logger.info(f"Published to self for next retry attempt {next_retry_count_for_pubsub} for identifier '{identifier}', stage '{stage}'.")

        retry_payload = {
            "customer": customer_id,
            "project": project_id_config_name,
            "identifier": identifier,
             # Add original_input_id to be passed along consistently if present
            "original_input_id": original_input_id
        }
        target_topic_name = None
        resolved_this_attempt = False

        if stage == "ingest-category-urls":
            target_topic_name = INGEST_CATEGORY_URLS_TOPIC_NAME
            retry_payload["csv_gcs_path"] = original_item_snapshot.get("csv_gcs_path") or original_input_id
            if not retry_payload["csv_gcs_path"]:
                 active_logger.error(f"Cannot retry '{stage}' for '{identifier}': missing CSV GCS path.")
                 publish_for_next_retry_attempt("Missing CSV GCS path for ingest-category-urls retry", original_item_snapshot)
                 return {"status": "retry_queued", "message": "Missing CSV GCS path"}

        elif stage == "discover-main-urls":
            target_topic_name = DISCOVER_MAIN_URLS_TOPIC_NAME
            retry_payload["category_url"] = category_url_from_payload or original_item_snapshot.get("category_url")
            if not retry_payload["category_url"]:
                active_logger.error(f"Cannot retry '{stage}' for '{identifier}': missing category_url.")
                publish_for_next_retry_attempt("Missing category_url for discover-main-urls retry", original_item_snapshot)
                return {"status": "retry_queued", "message": "Missing category_url"}

        elif stage == "extract-initial-metadata":
            target_topic_name = EXTRACT_INITIAL_METADATA_TOPIC_NAME
            retry_payload["main_url"] = main_url_from_payload or original_item_snapshot.get("main_url")
            retry_payload["category_url"] = category_url_from_payload or original_item_snapshot.get("category_url")
            if not retry_payload["main_url"]:
                active_logger.error(f"Cannot retry '{stage}' for '{identifier}': missing main_url.")
                publish_for_next_retry_attempt("Missing main_url for extract-initial-metadata retry", original_item_snapshot)
                return {"status": "retry_queued", "message": "Missing main_url"}
            retry_payload["category_page_metadata"] = original_item_snapshot.get("category_page_metadata", {})

        elif stage == "fetch-content":
            target_topic_name = FETCH_CONTENT_TOPIC_NAME
            retry_payload["main_url"] = main_url_from_payload or main_doc_data.get("main_url")
            if not retry_payload["main_url"]:
                active_logger.error(f"Cannot retry '{stage}' for '{identifier}': missing main_url in item snapshot or Firestore doc.")
                publish_for_next_retry_attempt("Missing main_url for fetch-content retry", original_item_snapshot, main_doc_data)
                return {"status": "retry_queued", "message": "Missing main_url"}

        elif stage == "generate-xml":
            target_topic_name = GENERATE_XML_TOPIC_NAME
            retry_payload["main_url"] = main_doc_data.get("main_url") # For logging/context in target function
            if not main_doc.exists or not main_doc_data.get("html_gcs_path"):
                active_logger.error(f"Cannot retry '{stage}' for '{identifier}': Firestore doc or html_gcs_path missing.")
                publish_for_next_retry_attempt("Missing Firestore data or GCS HTML path for generate-xml retry", original_item_snapshot, main_doc_data)
                return {"status": "retry_queued", "message": "Missing Firestore data or GCS HTML path"}
            # `identifier` is already in retry_payload and is the main lookup key for generate_xml

        elif stage == "generate-reports":
            target_topic_name = GENERATE_REPORTS_TOPIC_NAME
            retry_payload["report_date"] = original_item_snapshot.get("report_date", datetime.now(timezone.utc).strftime('%Y%m%d')) # Use "report_date" consistently
            retry_payload["processing_batch_id"] = original_item_snapshot.get("processing_batch_id", original_input_id)
            # 'identifier' here might be a report config name or a batch ID; ensure target function handles it.
            # For consistency, the 'identifier' from pubsub message is already in retry_payload.
            # If generate-reports uses a different kind of identifier, that should be in original_item_snapshot.

        else:
            active_logger.error(f"Unsupported retry stage '{stage}' for identifier '{identifier}'. Error: {current_error_message}")
            error_doc_ref.update({"status": f"Unresolvable_Unsupported_Stage"})
            return {"status": "failed_unsupported_stage", "message": f"Unsupported retry stage: {stage}"}

        if target_topic_name:
            try:
                # Ensure identifier is part of the payload if not already explicitly added for this stage
                if "identifier" not in retry_payload:
                    retry_payload["identifier"] = identifier

                publisher.publish(
                    publisher.topic_path(gcp_project_id, target_topic_name),
                    json.dumps(retry_payload, default=str).encode('utf-8')
                )
                active_logger.info(f"Successfully re-triggered stage '{stage}' for identifier '{identifier}' to topic '{target_topic_name}' with payload: {json.dumps(retry_payload, default=str)}")
                resolved_this_attempt = True
            except Exception as e_publish:
                active_logger.error(f"Failed to publish re-trigger message for '{identifier}' to '{target_topic_name}': {e_publish}", exc_info=True)
                publish_for_next_retry_attempt(f"Failed to publish to target topic {target_topic_name}: {e_publish}", original_item_snapshot, main_doc_data)
                return {"status": "retry_queued", "message": f"Failed to publish to target topic {target_topic_name}"}

        if resolved_this_attempt:
            error_doc_ref.update({
                "status": "Resolved_Retriggered",
                "resolved_at_timestamp": firestore.SERVER_TIMESTAMP,
                "resolution_details": f"Successfully re-triggered stage '{stage}' via retry_pipeline after {retry_count + 1} attempts."
            })
            active_logger.info(f"Marked error for '{identifier}' as Resolved_Retriggered for stage '{stage}'.")
            return {"status": "success_retriggered", "identifier": identifier, "stage": stage}
        else:
            active_logger.error(f"Retry logic for stage '{stage}' did not result in a re-trigger or explicit failure for '{identifier}'.")
            publish_for_next_retry_attempt(f"Internal retry logic incomplete for stage {stage}", original_item_snapshot, main_doc_data)
            return {"status": "retry_queued", "message": f"Internal retry logic incomplete for stage {stage}"}

    except Exception as e_critical:
        critical_error_message = sanitize_error_message(str(e_critical))
        # Use logger if active_logger is not defined due to early failure
        effective_logger = active_logger if 'active_logger' in locals() and active_logger.handlers else logger
        effective_logger.error(f"Critical error in retry_pipeline: {critical_error_message}", exc_info=True)
        
        current_identifier = data.get("identifier", "unknown_identifier") if 'data' in locals() else "unknown_identifier_at_critical"
        if 'error_doc_ref' in locals() and error_doc_ref: # Check if error_doc_ref was initialized
            try:
                error_doc_ref.update({
                    "error": f"Critical error in retry_pipeline itself for identifier {current_identifier}: {critical_error_message}",
                    "status": "Failed_Critical_In_Retry",
                    "last_retry_attempt_timestamp": firestore.SERVER_TIMESTAMP
                })
            except Exception as e_update_err_doc:
                effective_logger.error(f"Failed to update error document for {current_identifier} during critical error handling: {e_update_err_doc}")
        
        raise