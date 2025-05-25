import json
import base64
import logging
import os
import time
from google.cloud import firestore, pubsub_v1, storage
from apify_client import ApifyClient
from src.common.utils import generate_url_hash, setup_logging
from src.common.config import load_customer_config, load_project_config, get_secret
from src.common.helpers import generate_law_id, find_url_in_item, get_mapped_field, sanitize_error_message, analyze_error_with_vertex_ai, validate_html_content
from datetime import datetime, timezone
import functions_framework

logger = logging.getLogger(__name__)

APIFY_BATCH_SIZE = int(os.environ.get("APIFY_BATCH_SIZE", 50))
RETRY_TOPIC_NAME = "retry-pipeline"
MAX_API_RETRIES = 3
MAX_FIRESTORE_RETRIES = 3
RETRY_BACKOFF = 5
MAX_RETRIES = 3
MAX_DOCUMENT_SIZE = 1_000_000

@firestore.transactional
def update_sequence(transaction, sequence_doc_ref, sequence_number):
    """Update sequence number atomically."""
    snapshot = sequence_doc_ref.get(transaction=transaction)
    current_sequence = snapshot.get("last_sequence", 0) if snapshot.exists else 0
    if sequence_number > current_sequence + 1:
        transaction.set(sequence_doc_ref, {"last_sequence": sequence_number - 1}, merge=True)

@functions_framework.cloud_event
def retry_pipeline(cloud_event):
    active_logger = logger
    try:
        pubsub_message_data_encoded = cloud_event.data["message"]["data"]
        data = json.loads(base64.b64decode(pubsub_message_data_encoded).decode("utf-8"))
        active_logger.info(f"Decoded retry message: {json.dumps(data, default=str)}")

        customer_id = data.get("customer")
        project_id_config_name = data.get("project")
        dataset_id = data.get("dataset_id")
        dataset_type = data.get("dataset_type", "items")
        identifier = data.get("identifier")
        stage = data.get("stage")
        retry_count = int(data.get("retry_count", 0))

        if not all([customer_id, project_id_config_name, dataset_id, identifier, stage]):
            active_logger.error(f"Missing required fields: customer={customer_id}, project={project_id_config_name}, dataset_id={dataset_id}, identifier={identifier}, stage={stage}")
            raise ValueError("Missing required fields")

        active_logger = setup_logging(customer_id, project_id_config_name)
        active_logger.info(f"Processing retry for identifier '{identifier}', stage '{stage}', retry count {retry_count}")

        customer_config = load_customer_config(customer_id)
        project_config = load_project_config(project_id_config_name)
        gcp_project_id = customer_config.get("gcp_project_id", os.environ.get("GCP_PROJECT"))
        if not gcp_project_id:
            active_logger.error("GCP Project ID not found.")
            raise ValueError("GCP Project ID not configured")

        firestore_collection_name = project_config.get("firestore_collection")
        search_results_collection_name = project_config.get("search_results_collection", f"{firestore_collection_name}_search_results")
        firestore_db_id = project_config.get("firestore_database_id", "(default)")
        gcs_bucket_name = project_config.get("gcs_bucket")
        required_fields = project_config.get("required_fields", [])
        search_required_fields = project_config.get("search_required_fields", [])
        field_mappings = project_config.get("field_mappings", {})
        error_collection_name = f"{firestore_collection_name}_errors"
        apify_search_results_dataset_id = project_config.get("apify_search_results_dataset_id")
        apify_contents_dataset_id = project_config.get("apify_contents_dataset_id")

        db_options = {"project": gcp_project_id}
        if firestore_db_id != "(default)":
            db_options["database"] = firestore_db_id
        db = firestore.Client(**db_options)
        storage_client = storage.Client(project=gcp_project_id)
        bucket = storage_client.bucket(gcs_bucket_name)
        publisher = pubsub_v1.PublisherClient()
        retry_topic_path = publisher.topic_path(gcp_project_id, RETRY_TOPIC_NAME)

        error_doc_ref = db.collection(error_collection_name).document(identifier)
        error_doc = error_doc_ref.get()
        if not error_doc.exists:
            active_logger.error(f"No error document found for identifier '{identifier}'")
            return {"status": "error", "message": "No error document found"}

        error_data = error_doc.to_dict()
        original_item = data.get("item_data_snapshot") or error_data.get("original_item") or error_data.get("original_item_excerpt") or error_data.get("original_data") or {}
        error_message = error_data.get("error", "No error message recorded")
        dataset_fields = list(original_item.keys()) if isinstance(original_item, dict) else []
        active_logger.debug(f"Original item fields: {dataset_fields}")

        if retry_count >= MAX_RETRIES:
            active_logger.error(f"Max retries ({MAX_RETRIES}) reached for identifier '{identifier}' at stage '{stage}'")
            for attempt in range(MAX_FIRESTORE_RETRIES):
                try:
                    error_doc_ref.update({
                        "status": "Unresolvable",
                        "last_attempt_timestamp": firestore.SERVER_TIMESTAMP,
                        "final_error_message": error_message
                    })
                    break
                except Exception as e_update:
                    active_logger.warning(f"Firestore update failed on attempt {attempt + 1}: {str(e_update)}")
                    if attempt < MAX_FIRESTORE_RETRIES - 1:
                        time.sleep(RETRY_BACKOFF * (2 ** attempt))
                    else:
                        active_logger.error(f"Max retries reached for Firestore update: {str(e_update)}")
                        raise
            return {"status": "failed", "message": f"Max retries reached for {identifier}"}

        target_collection_name = search_results_collection_name if dataset_type == "search_results" else firestore_collection_name
        doc_ref = db.collection(target_collection_name).document(identifier)
        doc = doc_ref.get()
        doc_data = doc.to_dict() if doc.exists else {}

        llm_suggestion = analyze_error_with_vertex_ai(
            error_message, stage, field_mappings, dataset_fields, gcp_project_id, active_logger,
            extra_context={"doc_data": doc_data}
        )
        adjusted_params = llm_suggestion.get("adjusted_params", {})
        truncate_content_flag = adjusted_params.get("truncate_content", False)
        recheck_metadata_flag = adjusted_params.get("recheck_metadata", False)
        field_remapping_suggestion = adjusted_params.get("field_remapping", {})
        active_logger.info(f"LLM suggested params: truncate_content={truncate_content_flag}, recheck_metadata={recheck_metadata_flag}, field_remapping={field_remapping_suggestion}")

        current_field_mappings = field_mappings.copy()
        if isinstance(field_remapping_suggestion, dict):
            for field, new_source in field_remapping_suggestion.items():
                if isinstance(new_source, str):
                    current_field_mappings[field] = {"source": new_source, "type": "direct"}
                    active_logger.info(f"Updated field mapping for retry: {field} -> {new_source}")
                else:
                    active_logger.warning(f"Invalid new_source '{new_source}' for field '{field}'")

        for attempt in range(MAX_FIRESTORE_RETRIES):
            try:
                error_doc_ref.update({
                    "llm_suggestion_history": firestore.ArrayUnion([llm_suggestion]),
                    "last_attempt_timestamp": firestore.SERVER_TIMESTAMP,
                    "retry_attempt_details": firestore.ArrayUnion([{
                        "timestamp": firestore.SERVER_TIMESTAMP,
                        "retry_count": retry_count + 1,
                        "stage": stage,
                        "llm_suggestion": llm_suggestion,
                        "applied_params": {
                            "truncate_content": truncate_content_flag,
                            "recheck_metadata": recheck_metadata_flag,
                            "field_remapping": field_remapping_suggestion
                        }
                    }])
                })
                break
            except Exception as e_update:
                active_logger.warning(f"Firestore update failed on attempt {attempt + 1}: {str(e_update)}")
                if attempt < MAX_FIRESTORE_RETRIES - 1:
                    time.sleep(RETRY_BACKOFF * (2 ** attempt))
                else:
                    active_logger.error(f"Max retries reached for Firestore update: {str(e_update)}")
                    raise

        next_retry_count = retry_count + 1

        def publish_for_next_retry(current_error_msg="Retry attempt failed", item_snapshot=None):
            for attempt in range(MAX_FIRESTORE_RETRIES):
                try:
                    error_doc_ref.update({
                        "retry_count": next_retry_count,
                        "error": f"Retry attempt {next_retry_count} for stage '{stage}' failed: {sanitize_error_message(current_error_msg)}",
                        "status": "Retrying"
                    })
                    break
                except Exception as e_update:
                    active_logger.warning(f"Firestore update failed on attempt {attempt + 1}: {str(e_update)}")
                    if attempt < MAX_FIRESTORE_RETRIES - 1:
                        time.sleep(RETRY_BACKOFF * (2 ** attempt))
                    else:
                        active_logger.error(f"Max retries reached for Firestore update: {str(e_update)}")
                        raise
            retry_message_payload = {
                "customer": customer_id,
                "project": project_id_config_name,
                "dataset_id": dataset_id,
                "dataset_type": dataset_type,
                "identifier": identifier,
                "stage": stage,
                "retry_count": next_retry_count,
                "item_data_snapshot": item_snapshot or original_item,
                "doc_data": doc_data
            }
            publisher.publish(retry_topic_path, json.dumps(retry_message_payload, default=str).encode('utf-8'))
            active_logger.info(f"Published retry message for identifier '{identifier}', stage '{stage}', attempt {next_retry_count}")

        if stage == "extract_metadata":
            if not original_item:
                active_logger.error(f"Cannot retry 'extract_metadata' for '{identifier}': missing original_item")
                for attempt in range(MAX_FIRESTORE_RETRIES):
                    try:
                        error_doc_ref.update({"status": "Unresolvable", "error": "Missing original_item"})
                        break
                    except Exception as e_update:
                        if attempt < MAX_FIRESTORE_RETRIES - 1:
                            time.sleep(RETRY_BACKOFF * (2 ** attempt))
                        else:
                            raise
                return {"status": "failed", "message": "Missing original_item"}

            try:
                content_url, _ = find_url_in_item(original_item, active_logger)
                if not content_url:
                    active_logger.error(f"No valid URL for retry item {identifier}")
                    publish_for_next_retry("No valid URL found", original_item)
                    return {"status": "retry_queued", "identifier": identifier, "stage": stage, "message": "No valid URL found"}

                # Re-fetch from Apify if recheck_metadata is suggested
                if recheck_metadata_flag and apify_contents_dataset_id:
                    apify_key = get_secret(project_config.get("apify_api_key_secret", "apify-api-key"), gcp_project_id)
                    apify_client = ApifyClient(apify_key)
                    try:
                        items = apify_client.dataset(apify_contents_dataset_id).list_items(
                            offset=0, limit=APIFY_BATCH_SIZE, fields=["url", "htmlContent", "html", "content"]
                        ).items
                        for item in items:
                            item_url, _ = find_url_in_item(item, active_logger)
                            if item_url and generate_url_hash(item_url) == identifier:
                                original_item.update(item)
                                active_logger.info(f"Updated original_item for {identifier} from Apify dataset")
                                break
                    except Exception as e_apify:
                        active_logger.warning(f"Failed to re-fetch from Apify for {identifier}: {str(e_apify)}")

                metadata = {
                    "scrape_date": original_item.get("extractionTimestamp", datetime.now(timezone.utc)),
                    "processed_at_extract_metadata_retry": firestore.SERVER_TIMESTAMP,
                    "retry_attempt_count": next_retry_count,
                    "main_url": content_url,
                    "dataset_id_source": dataset_id
                }

                fields_to_process = search_required_fields if dataset_type == "search_results" else required_fields
                sequence_doc_ref = db.collection(f"{firestore_collection_name}_metadata").document("sequence")
                transaction = db.transaction()
                sequence_doc = sequence_doc_ref.get()
                sequence_number = sequence_doc.to_dict().get("last_sequence", 0) + 1 if sequence_doc.exists else 1

                for field_name in fields_to_process:
                    value = get_mapped_field(
                        original_item, field_name, current_field_mappings, logger_instance=active_logger,
                        extra_context={"sequence_number": sequence_number, "project": project_id_config_name}
                    )
                    if value is not None and value != "Not Available":
                        metadata[field_name] = value
                        active_logger.debug(f"Mapped field '{field_name}' to value: {value}")
                    else:
                        active_logger.warning(f"Field '{field_name}' not mapped for {identifier}")
                    if field_name == "Law-ID" and metadata.get("Law-ID") != "Not Available":
                        sequence_number += 1

                if metadata.get("Law-ID") == "Not Available":
                    active_logger.error(f"Invalid Law-ID for retry item {identifier}")
                    publish_for_next_retry("Invalid Law-ID generated", original_item)
                    return {"status": "retry_queued", "identifier": identifier, "stage": stage, "message": "Invalid Law-ID generated"}

                # Store HTML in GCS if available
                html_content = original_item.get("htmlContent") or original_item.get("html") or original_item.get("content")
                html_path_gcs = None
                if html_content:
                    html_content = validate_html_content(html_content, active_logger)
                    if html_content:
                        if truncate_content_flag and len(html_content.encode('utf-8')) > MAX_DOCUMENT_SIZE:
                            html_content = html_content[:MAX_DOCUMENT_SIZE // 2]
                            active_logger.info(f"Truncated HTML content for {identifier} to {len(html_content.encode('utf-8'))} bytes")
                        safe_project_id_path = "".join(c if c.isalnum() else '_' for c in project_id_config_name)
                        date_str = datetime.now().strftime('%Y%m%d')
                        gcs_temp_html_path = f"temp_html/{safe_project_id_path}/{date_str}"
                        destination_blob_name = f"{gcs_temp_html_path}/{identifier}_temp.html"
                        blob = bucket.blob(destination_blob_name)
                        for attempt in range(MAX_FIRESTORE_RETRIES):
                            try:
                                blob.upload_from_string(html_content, content_type="text/html; charset=utf-8")
                                html_path_gcs = f"gs://{gcs_bucket_name}/{destination_blob_name}"
                                metadata["html_path"] = html_path_gcs
                                active_logger.info(f"Stored uncompressed HTML for {identifier} to GCS: {html_path_gcs}")
                                break
                            except Exception as e_upload:
                                active_logger.warning(f"GCS upload failed on attempt {attempt + 1}: {str(e_upload)}")
                                if attempt < MAX_FIRESTORE_RETRIES - 1:
                                    time.sleep(RETRY_BACKOFF * (2 ** attempt))
                                else:
                                    active_logger.error(f"Max retries reached for GCS upload: {str(e_upload)}")
                                    publish_for_next_retry(f"GCS upload failed: {str(e_upload)}", original_item)
                                    return {"status": "retry_queued", "identifier": identifier, "stage": stage, "message": f"GCS upload failed: {str(e_upload)}"}

                serialized_metadata_str = json.dumps(metadata, default=str)
                serialized_size = len(serialized_metadata_str.encode('utf-8'))
                if serialized_size > MAX_DOCUMENT_SIZE:
                    active_logger.warning(f"Document {identifier} exceeds Firestore size limit ({serialized_size} bytes)")
                    gcs_path = f"oversized_metadata/{project_id_config_name}/{dataset_id}/{identifier}.json"
                    blob = bucket.blob(gcs_path)
                    for attempt in range(MAX_FIRESTORE_RETRIES):
                        try:
                            blob.upload_from_string(serialized_metadata_str, content_type="application/json")
                            break
                        except Exception as e_upload:
                            if attempt < MAX_FIRESTORE_RETRIES - 1:
                                time.sleep(RETRY_BACKOFF * (2 ** attempt))
                            else:
                                raise
                    minimal_metadata = {
                        "Law-ID": metadata.get("Law-ID", "Not Available"),
                        "main_url": metadata.get("main_url", "Not Available"),
                        "scrape_date": metadata.get("scrape_date"),
                        "processed_at_extract_metadata_retry": firestore.SERVER_TIMESTAMP,
                        "dataset_id_source": dataset_id,
                        "full_metadata_gcs_path": f"gs://{gcs_bucket_name}/{gcs_path}",
                        "status_notice": "Full metadata stored in GCS due to size"
                    }
                    for attempt in range(MAX_FIRESTORE_RETRIES):
                        try:
                            db.collection(target_collection_name).document(identifier).set(minimal_metadata, merge=True)
                            break
                        except Exception as e_update:
                            if attempt < MAX_FIRESTORE_RETRIES - 1:
                                time.sleep(RETRY_BACKOFF * (2 ** attempt))
                            else:
                                raise
                else:
                    for attempt in range(MAX_FIRESTORE_RETRIES):
                        try:
                            db.collection(target_collection_name).document(identifier).set(metadata, merge=True)
                            break
                        except Exception as e_update:
                            active_logger.warning(f"Firestore update failed on attempt {attempt + 1}: {str(e_update)}")
                            if attempt < MAX_FIRESTORE_RETRIES - 1:
                                time.sleep(RETRY_BACKOFF * (2 ** attempt))
                            else:
                                active_logger.error(f"Max retries reached for Firestore update: {str(e_update)}")
                                publish_for_next_retry(f"Firestore update failed: {str(e_update)}", original_item)
                                return {"status": "retry_queued", "identifier": identifier, "stage": stage, "message": f"Firestore update failed: {str(e_update)}"}

                if sequence_number > 1:
                    update_sequence(transaction, sequence_doc_ref, sequence_number)

                publisher.publish(
                    publisher.topic_path(gcp_project_id, "fix-image-urls"),
                    json.dumps({
                        "customer": customer_id,
                        "project": project_id_config_name,
                        "dataset_id": dataset_id,
                        "dataset_type": dataset_type,
                        "identifier": identifier,
                        "main_url": content_url,
                        "html_path": html_path_gcs,
                        "date": datetime.now().strftime('%Y%m%d'),
                        "apify_dataset_id_source": dataset_id
                    }).encode('utf-8')
                )

                for attempt in range(MAX_FIRESTORE_RETRIES):
                    try:
                        error_doc_ref.update({
                            "status": "Resolved",
                            "resolved_at_timestamp": firestore.SERVER_TIMESTAMP,
                            "resolution_details": "Successfully processed at extract_metadata stage during retry"
                        })
                        break
                    except Exception as e_update:
                        active_logger.warning(f"Firestore update failed on attempt {attempt + 1}: {str(e_update)}")
                        if attempt < MAX_FIRESTORE_RETRIES - 1:
                            time.sleep(RETRY_BACKOFF * (2 ** attempt))
                        else:
                            active_logger.error(f"Max retries reached for Firestore update: {str(e_update)}")
                            raise
                return {"status": "success", "identifier": identifier, "stage": stage, "message": "extract_metadata retried successfully"}

            except Exception as e:
                active_logger.error(f"Retry attempt {next_retry_count} failed for '{identifier}' at 'extract_metadata': {str(e)}", exc_info=True)
                publish_for_next_retry(str(e), original_item)
                return {"status": "retry_queued", "identifier": identifier, "stage": stage, "message": f"extract_metadata retry failed: {str(e)}"}

        elif stage == "fix_image_urls":
            try:
                if not doc.exists:
                    active_logger.error(f"No Firestore document for identifier {identifier}")
                    publish_for_next_retry("No Firestore document found")
                    return {"status": "retry_queued", "identifier": identifier, "stage": stage, "message": "No Firestore document found"}

                doc_data = doc.to_dict()
                main_url = doc_data.get("main_url", original_item.get("main_url", original_item.get("mainUrl")))
                html_path = doc_data.get("html_path")
                if not main_url or not html_path:
                    active_logger.error(f"Missing main_url or html_path for identifier {identifier}")
                    publish_for_next_retry("Missing main_url or html_path")
                    return {"status": "retry_queued", "identifier": identifier, "stage": stage, "message": "Missing main_url or html_path"}

                # Verify HTML exists in GCS
                blob = bucket.blob(html_path.replace(f"gs://{gcs_bucket_name}/", ""))
                if not blob.exists():
                    active_logger.error(f"HTML file {html_path} not found in GCS for {identifier}")
                    publish_for_next_retry("HTML file not found in GCS")
                    return {"status": "retry_queued", "identifier": identifier, "stage": stage, "message": "HTML file not found in GCS"}

                publisher.publish(
                    publisher.topic_path(gcp_project_id, "fix-image-urls"),
                    json.dumps({
                        "customer": customer_id,
                        "project": project_id_config_name,
                        "dataset_id": dataset_id,
                        "dataset_type": dataset_type,
                        "identifier": identifier,
                        "main_url": main_url,
                        "html_path": html_path,
                        "date": datetime.now().strftime('%Y%m%d'),
                        "apify_dataset_id_source": dataset_id,
                        "apify_run_id_trigger": original_item.get("apify_run_id_trigger", "unknown")
                    }).encode('utf-8')
                )

                for attempt in range(MAX_FIRESTORE_RETRIES):
                    try:
                        error_doc_ref.update({
                            "status": "Resolved",
                            "resolved_at_timestamp": firestore.SERVER_TIMESTAMP,
                            "resolution_details": "Successfully triggered fix_image_urls retry"
                        })
                        break
                    except Exception as e_update:
                        active_logger.warning(f"Firestore update failed on attempt {attempt + 1}: {str(e_update)}")
                        if attempt < MAX_FIRESTORE_RETRIES - 1:
                            time.sleep(RETRY_BACKOFF * (2 ** attempt))
                        else:
                            active_logger.error(f"Max retries reached for Firestore update: {str(e_update)}")
                            raise
                return {"status": "success", "identifier": identifier, "stage": stage, "message": "fix_image_urls retried successfully"}

            except Exception as e:
                active_logger.error(f"Retry attempt {next_retry_count} failed for '{identifier}' at 'fix_image_urls': {str(e)}", exc_info=True)
                publish_for_next_retry(str(e))
                return {"status": "retry_queued", "identifier": identifier, "stage": stage, "message": f"fix_image_urls retry failed: {str(e)}"}

        elif stage == "store_html":
            try:
                if not doc.exists:
                    active_logger.error(f"No Firestore document for identifier {identifier}")
                    publish_for_next_retry("No Firestore document found")
                    return {"status": "retry_queued", "identifier": identifier, "stage": stage, "message": "No Firestore document found"}

                doc_data = doc.to_dict()
                main_url = doc_data.get("main_url", original_item.get("main_url", original_item.get("mainUrl")))
                html_path = doc_data.get("html_path")
                if not main_url or not html_path:
                    active_logger.error(f"Missing main_url or html_path for identifier {identifier}")
                    publish_for_next_retry("Missing main_url or html_path")
                    return {"status": "retry_queued", "identifier": identifier, "stage": stage, "message": "Missing main_url or html_path"}

                # Fetch HTML from GCS
                fixed_html_content = None
                try:
                    blob = bucket.blob(html_path.replace(f"gs://{gcs_bucket_name}/", ""))
                    if blob.exists():
                        fixed_html_content = blob.download_as_text()
                        fixed_html_content = validate_html_content(fixed_html_content, active_logger)
                        if not fixed_html_content:
                            active_logger.error(f"Invalid HTML content from GCS for {identifier}")
                            publish_for_next_retry("Invalid HTML content from GCS")
                            return {"status": "retry_queued", "identifier": identifier, "stage": stage, "message": "Invalid HTML content from GCS"}
                    else:
                        active_logger.error(f"HTML file {html_path} not found in GCS for {identifier}")
                        publish_for_next_retry("HTML file not found in GCS")
                        return {"status": "retry_queued", "identifier": identifier, "stage": stage, "message": "HTML file not found in GCS"}
                except Exception as e_gcs:
                    active_logger.error(f"Failed to fetch HTML from GCS for {identifier}: {str(e_gcs)}", exc_info=True)
                    publish_for_next_retry(f"GCS fetch failed: {str(e_gcs)}")
                    return {"status": "retry_queued", "identifier": identifier, "stage": stage, "message": f"GCS fetch failed: {str(e_gcs)}"}

                # Re-fetch augmentation data if recheck_metadata is suggested
                if recheck_metadata_flag and apify_search_results_dataset_id:
                    apify_key = get_secret(project_config.get("apify_api_key_secret", "apify-api-key"), gcp_project_id)
                    apify_client = ApifyClient(apify_key)
                    try:
                        items = apify_client.dataset(apify_search_results_dataset_id).list_items(
                            offset=0, limit=APIFY_BATCH_SIZE, fields=["url", "pdfLink", "pdf_link"]
                        ).items
                        for item in items:
                            item_url, _ = find_url_in_item(item, active_logger)
                            if item_url and generate_url_hash(item_url) == identifier:
                                original_item.update(item)
                                active_logger.info(f"Updated original_item for {identifier} from Apify search dataset")
                                break
                    except Exception as e_apify:
                        active_logger.warning(f"Failed to re-fetch from Apify for {identifier}: {str(e_apify)}")

                publisher.publish(
                    publisher.topic_path(gcp_project_id, "store-html"),
                    json.dumps({
                        "customer": customer_id,
                        "project": project_id_config_name,
                        "dataset_id": dataset_id,
                        "dataset_type": dataset_type,
                        "identifier": identifier,
                        "main_url": main_url,
                        "fixed_html_content": fixed_html_content[:500000] if truncate_content_flag else fixed_html_content,
                        "images_fixed_count": doc_data.get("images_fixed_count", 0),
                        "date": datetime.now().strftime('%Y%m%d'),
                        "apify_dataset_id_source": dataset_id,
                        "apify_run_id_trigger": original_item.get("apify_run_id_trigger", "unknown")
                    }).encode('utf-8')
                )

                for attempt in range(MAX_FIRESTORE_RETRIES):
                    try:
                        error_doc_ref.update({
                            "status": "Resolved",
                            "resolved_at_timestamp": firestore.SERVER_TIMESTAMP,
                            "resolution_details": "Successfully triggered store_html retry"
                        })
                        break
                    except Exception as e_update:
                        active_logger.warning(f"Firestore update failed on attempt {attempt + 1}: {str(e_update)}")
                        if attempt < MAX_FIRESTORE_RETRIES - 1:
                            time.sleep(RETRY_BACKOFF * (2 ** attempt))
                        else:
                            active_logger.error(f"Max retries reached for Firestore update: {str(e_update)}")
                            raise
                return {"status": "success", "identifier": identifier, "stage": stage, "message": "store_html retried successfully"}

            except Exception as e:
                active_logger.error(f"Retry attempt {next_retry_count} failed for '{identifier}' at 'store_html': {str(e)}", exc_info=True)
                publish_for_next_retry(str(e))
                return {"status": "retry_queued", "identifier": identifier, "stage": stage, "message": f"store_html retry failed: {str(e)}"}

        elif stage == "generate_xml":
            try:
                if not doc.exists:
                    active_logger.error(f"No Firestore document for identifier {identifier}")
                    publish_for_next_retry("No Firestore document found")
                    return {"status": "retry_queued", "identifier": identifier, "stage": stage, "message": "No Firestore document found"}

                doc_data = doc.to_dict()
                main_url = doc_data.get("main_url", original_item.get("main_url", original_item.get("mainUrl")))
                html_path = doc_data.get("html_path")
                if not main_url or not html_path:
                    active_logger.error(f"Missing main_url or html_path for identifier {identifier}")
                    publish_for_next_retry("Missing main_url or html_path")
                    return {"status": "retry_queued", "identifier": identifier, "stage": stage, "message": "Missing main_url or html_path"}

                # Verify HTML exists in GCS
                blob = bucket.blob(html_path.replace(f"gs://{gcs_bucket_name}/", ""))
                if not blob.exists():
                    active_logger.error(f"HTML file {html_path} not found in GCS for {identifier}")
                    publish_for_next_retry("HTML file not found in GCS")
                    return {"status": "retry_queued", "identifier": identifier, "stage": stage, "message": "HTML file not found in GCS"}

                publisher.publish(
                    publisher.topic_path(gcp_project_id, "generate-xml"),
                    json.dumps({
                        "customer": customer_id,
                        "project": project_id_config_name,
                        "dataset_id": dataset_id,
                        "dataset_type": dataset_type,
                        "identifier": identifier,
                        "main_url": main_url,
                        "html_path": html_path,
                        "date": datetime.now().strftime('%Y%m%d'),
                        "apify_dataset_id_source": dataset_id,
                        "apify_run_id_trigger": original_item.get("apify_run_id_trigger", "unknown")
                    }).encode('utf-8')
                )

                for attempt in range(MAX_FIRESTORE_RETRIES):
                    try:
                        error_doc_ref.update({
                            "status": "Resolved",
                            "resolved_at_timestamp": firestore.SERVER_TIMESTAMP,
                            "resolution_details": "Successfully triggered generate_xml retry"
                        })
                        break
                    except Exception as e_update:
                        active_logger.warning(f"Firestore update failed on attempt {attempt + 1}: {str(e_update)}")
                        if attempt < MAX_FIRESTORE_RETRIES - 1:
                            time.sleep(RETRY_BACKOFF * (2 ** attempt))
                        else:
                            active_logger.error(f"Max retries reached for Firestore update: {str(e_update)}")
                            raise
                return {"status": "success", "identifier": identifier, "stage": stage, "message": "generate_xml retried successfully"}

            except Exception as e:
                active_logger.error(f"Retry attempt {next_retry_count} failed for '{identifier}' at 'generate_xml': {str(e)}", exc_info=True)
                publish_for_next_retry(str(e))
                return {"status": "retry_queued", "identifier": identifier, "stage": stage, "message": f"generate_xml retry failed: {str(e)}"}

        elif stage == "generate_reports":
            try:
                date_str = data.get("date", datetime.now().strftime('%Y%m%d'))
                publisher.publish(
                    publisher.topic_path(gcp_project_id, "generate-reports"),
                    json.dumps({
                        "customer": customer_id,
                        "project": project_id_config_name,
                        "dataset_id": dataset_id,
                        "dataset_type": dataset_type,
                        "identifier": identifier,
                        "date": date_str
                    }).encode('utf-8')
                )

                for attempt in range(MAX_FIRESTORE_RETRIES):
                    try:
                        error_doc_ref.update({
                            "status": "Resolved",
                            "resolved_at_timestamp": firestore.SERVER_TIMESTAMP,
                            "resolution_details": "Successfully triggered generate_reports retry"
                        })
                        break
                    except Exception as e_update:
                        active_logger.warning(f"Firestore update failed on attempt {attempt + 1}: {str(e_update)}")
                        if attempt < MAX_FIRESTORE_RETRIES - 1:
                            time.sleep(RETRY_BACKOFF * (2 ** attempt))
                        else:
                            active_logger.error(f"Max retries reached for Firestore update: {str(e_update)}")
                            raise
                return {"status": "success", "identifier": identifier, "stage": stage, "message": "generate_reports retried successfully"}

            except Exception as e:
                active_logger.error(f"Retry attempt {next_retry_count} failed for '{identifier}' at 'generate_reports': {str(e)}", exc_info=True)
                publish_for_next_retry(str(e))
                return {"status": "retry_queued", "identifier": identifier, "stage": stage, "message": f"generate_reports retry failed: {str(e)}"}

        else:
            active_logger.error(f"Unsupported retry stage '{stage}' for identifier '{identifier}'")
            for attempt in range(MAX_FIRESTORE_RETRIES):
                try:
                    error_doc_ref.update({"status": "Unresolvable", "error": f"Unsupported retry stage: {stage}"})
                    break
                except Exception as e_update:
                    active_logger.warning(f"Firestore update failed on attempt {attempt + 1}: {str(e_update)}")
                    if attempt < MAX_FIRESTORE_RETRIES - 1:
                        time.sleep(RETRY_BACKOFF * (2 ** attempt))
                    else:
                        active_logger.error(f"Max retries reached for Firestore update: {str(e_update)}")
                        raise
            return {"status": "failed", "message": f"Unsupported retry stage: {stage}"}

    except Exception as e:
        active_logger.error(f"Critical error in retry_pipeline: {str(e)}", exc_info=True)
        if 'error_doc_ref' in locals():
            for attempt in range(MAX_FIRESTORE_RETRIES):
                try:
                    error_doc_ref.update({
                        "error": f"Critical error: {sanitize_error_message(str(e))}",
                        "status": "Failed",
                        "last_attempt_timestamp": firestore.SERVER_TIMESTAMP
                    })
                    break
                except Exception as e_update:
                    active_logger.warning(f"Firestore update failed on attempt {attempt + 1}: {str(e_update)}")
                    if attempt < MAX_FIRESTORE_RETRIES - 1:
                        time.sleep(RETRY_BACKOFF * (2 ** attempt))
                    else:
                        active_logger.error(f"Max retries reached for Firestore update: {str(e_update)}")
                        raise
        raise