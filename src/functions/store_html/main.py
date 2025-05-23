import json
import base64
import logging
import os
from google.cloud import firestore, pubsub_v1, storage
from apify_client import ApifyClient
from src.common.utils import generate_url_hash, compress_and_upload, setup_logging
from src.common.config import load_customer_config, load_project_config, get_secret
from datetime import datetime
import functions_framework
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)
RETRY_TOPIC_NAME = "retry-pipeline"
NEXT_STEP_TOPIC_NAME = "fix-image-urls"
MAX_API_RETRIES = 3
RETRY_BACKOFF = 5  # seconds

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
        logger.warning(f"Error extracting title from content: {e}")
        return "untitled"

def generate_identifier(item, field_mappings):
    """Generate a unique identifier for an item using mapped fields."""
    url_fields = field_mappings.get("Law-ID", {}).get("source", "htmlUrl || url || mainUrl").split(" || ")
    for field in url_fields:
        field = field.strip()
        if item.get(field):
            return generate_url_hash(item[field])
    return generate_url_hash(f"no-id-{datetime.now().isoformat()}")

def get_mapped_field(item, field, field_mappings, logger_instance=None):
    """Get the value for a field based on its mapping in field_mappings."""
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
            url_fields = source.split("(")[1].split(")")[0].split(" || ")
            for url_field in url_fields:
                url_field = url_field.strip()
                if item.get(url_field):
                    return item[url_field].split("/")[-1].replace(".html", "")
            return ""
        return "Not Available"
    else:
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
            if item.get(source_field):
                return item[source_field]
        logger_instance.warning(f"No valid source field found for '{field}' in item: {list(item.keys())}")
        return "Not Available"

@functions_framework.cloud_event
def store_html(cloud_event):
    active_logger = logger
    try:
        pubsub_data_encoded = cloud_event.data.get("message", {}).get("data")
        if not pubsub_data_encoded:
            if isinstance(cloud_event.data, dict) and "customer" in cloud_event.data:
                data = cloud_event.data
                active_logger.info("Interpreting cloud_event.data directly as JSON payload.")
            else:
                raise ValueError("Pub/Sub message data not found or in unexpected format.")

        if pubsub_data_encoded:
            data = json.loads(base64.b64decode(pubsub_data_encoded).decode("utf-8"))
            active_logger.info(f"Decoded Pub/Sub message: {data}")

        customer_id = data.get("customer")
        project_id = data.get("project")
        dataset_id = data.get("dataset_id")
        dataset_type = data.get("dataset_type", "items")
        offset = int(data.get("offset", 0))
        count = int(data.get("count", 50))

        if not all([customer_id, project_id, dataset_id]):
            raise ValueError("Missing required fields (customer, project, dataset_id)")

        active_logger = setup_logging(customer_id, project_id)
        active_logger.info(f"Starting store_html for customer: {customer_id}, project: {project_id}, dataset_id: {dataset_id}, type: {dataset_type}, offset: {offset}, count: {count}")

        customer_config = load_customer_config(customer_id)
        project_config = load_project_config(project_id)

        gcp_project_id = customer_config.get("gcp_project_id", os.environ.get("GCP_PROJECT"))
        if not gcp_project_id:
            raise ValueError("GCP Project ID not configured")

        gcs_bucket = project_config.get("gcs_bucket")
        firestore_collection = project_config.get("firestore_collection")
        search_results_collection = project_config.get("search_results_collection", f"{firestore_collection}_search_results")
        firestore_db_id = project_config.get("firestore_database_id", "(default)")
        batch_size = int(project_config.get("apify_batch_size", 10))
        error_collection = f"{firestore_collection}_errors"
        field_mappings = project_config.get("field_mappings", {})

        apify_key = get_secret(project_config.get("apify_api_key_secret", "apify-api-key"), gcp_project_id)
        apify_client = ApifyClient(apify_key)
        storage_client = storage.Client(project=gcp_project_id)
        bucket = storage_client.bucket(gcs_bucket)
        publisher = pubsub_v1.PublisherClient()

        db_options = {"project": gcp_project_id}
        if firestore_db_id != "(default)":
            db_options["database"] = firestore_db_id
        db = firestore.Client(**db_options)

        results = []
        error_batch = db.batch()
        error_count = 0
        current_date_str = data.get("date", datetime.now().strftime('%Y%m%d'))
        items_processed = 0

        while items_processed < count:
            items_to_fetch = min(batch_size, count - items_processed)
            if items_to_fetch <= 0:
                break
            current_apify_offset = offset + items_processed
            active_logger.info(f"Fetching Apify data. Dataset: {dataset_id}, Offset: {current_apify_offset}, Limit: {items_to_fetch}")

            items = None
            for attempt in range(MAX_API_RETRIES):
                try:
                    response = apify_client.dataset(dataset_id).list_items(offset=current_apify_offset, limit=items_to_fetch)
                    items = response.items
                    active_logger.debug(f"Apify API call successful on attempt {attempt + 1}")
                    break
                except Exception as e:
                    active_logger.warning(f"Apify API call failed on attempt {attempt + 1}: {str(e)}")
                    if attempt < MAX_API_RETRIES - 1:
                        time.sleep(RETRY_BACKOFF * (2 ** attempt))
                    else:
                        active_logger.error(f"Max retries reached for Apify API: {str(e)}")
                        results.append({"offset": current_apify_offset, "limit": items_to_fetch, "status": f"Flagged for retry (Apify fetch error): {str(e)}"})
                        break

            if not items:
                active_logger.info(f"No more items found in Apify dataset {dataset_id} at offset {current_apify_offset}.")
                break

            active_logger.info(f"Fetched {len(items)} items from Apify.")
            firestore_batch = db.batch()

            for item in items:
                identifier = generate_identifier(item, field_mappings)
                raw_url_for_id = get_mapped_field(item, "Law-ID", field_mappings, active_logger)
                item_url_for_logging = raw_url_for_id if raw_url_for_id != "Not Available" else "N/A"

                try:
                    target_collection_name = search_results_collection if dataset_type == "search_results" else firestore_collection
                    doc_ref = db.collection(target_collection_name).document(identifier)
                    doc = doc_ref.get()

                    if not doc.exists:
                        active_logger.warning(f"No Firestore document found for identifier {identifier} (URL: {item_url_for_logging}).")
                        error_doc_data = {
                            "identifier": identifier,
                            "url_used_for_id": item_url_for_logging,
                            "error": "No Firestore document found from previous step",
                            "stage": "store_html",
                            "retry_count": 0,
                            "original_item": item,
                            "timestamp": firestore.SERVER_TIMESTAMP
                        }
                        error_batch.set(db.collection(error_collection).document(identifier), error_doc_data)
                        error_count += 1
                        results.append({"identifier": identifier, "url": item_url_for_logging, "status": "Flagged for retry (missing base document)"})
                        publisher.publish(
                            publisher.topic_path(gcp_project_id, RETRY_TOPIC_NAME),
                            json.dumps({
                                "customer": customer_id,
                                "project": project_id,
                                "dataset_id": dataset_id,
                                "dataset_type": dataset_type,
                                "identifier": identifier,
                                "stage": "extract_metadata",
                                "retry_count": 0
                            }).encode('utf-8')
                        )
                        continue

                    doc_data = doc.to_dict()
                    content = get_mapped_field(item, "Content", field_mappings, active_logger)
                    url_for_gcs_path = doc_data.get("URL", get_mapped_field(item, "URL", field_mappings, active_logger))
                    title_for_filename = doc_data.get("Title", get_mapped_field(item, "Title", field_mappings, active_logger))

                    if title_for_filename == "Not Available":
                        title_for_filename = extract_title_from_html(content if content != "Not Available" else "") or "untitled"

                    filename_base = "".join(c if c.isalnum() or c in (' ', '_', '-') else '_' for c in title_for_filename).replace(" ", "_")[:150]

                    if not url_for_gcs_path or url_for_gcs_path == "Not Available":
                        active_logger.error(f"No valid URL for item (identifier: {identifier}).")
                        error_batch.set(db.collection(error_collection).document(identifier), {
                            "identifier": identifier,
                            "error": "No valid URL in Firestore doc or Apify item",
                            "stage": "store_html",
                            "retry_count": 0,
                            "original_item": item,
                            "timestamp": firestore.SERVER_TIMESTAMP
                        }, merge=True)
                        error_count += 1
                        results.append({"identifier": identifier, "url": item_url_for_logging, "status": "Flagged for retry (no valid URL)"})
                        publisher.publish(
                            publisher.topic_path(gcp_project_id, RETRY_TOPIC_NAME),
                            json.dumps({
                                "customer": customer_id,
                                "project": project_id,
                                "dataset_id": dataset_id,
                                "dataset_type": dataset_type,
                                "identifier": identifier,
                                "stage": "store_html",
                                "retry_count": 0
                            }).encode('utf-8')
                        )
                        continue

                    if not content or content == "Not Available":
                        active_logger.error(f"No content for {url_for_gcs_path} (identifier: {identifier}).")
                        error_batch.set(db.collection(error_collection).document(identifier), {
                            "identifier": identifier,
                            "error": "No content in Apify item",
                            "stage": "store_html",
                            "retry_count": 0,
                            "original_item": item,
                            "timestamp": firestore.SERVER_TIMESTAMP
                        }, merge=True)
                        error_count += 1
                        results.append({"identifier": identifier, "url": item_url_for_logging, "status": "Flagged for retry (no content)"})
                        publisher.publish(
                            publisher.topic_path(gcp_project_id, RETRY_TOPIC_NAME),
                            json.dumps({
                                "customer": customer_id,
                                "project": project_id,
                                "dataset_id": dataset_id,
                                "dataset_type": dataset_type,
                                "identifier": identifier,
                                "stage": "store_html",
                                "retry_count": 0
                            }).encode('utf-8')
                        )
                        continue

                    safe_project_id = "".join(c if c.isalnum() else '_' for c in project_id)
                    safe_dataset_id_part = "".join(c if c.isalnum() else '_' for c in dataset_id[:20])
                    base_folder = 'search_results' if dataset_type == 'search_results' else 'pending'
                    destination_blob_name = f"{base_folder}/{safe_project_id}/{current_date_str}/{safe_dataset_id_part}/{identifier}_{filename_base}.{'txt' if dataset_type == 'search_results' else 'html.gz'}"

                    try:
                        if dataset_type == "search_results":
                            blob = bucket.blob(destination_blob_name)
                            blob.upload_from_string(content, content_type="text/plain; charset=utf-8")
                        else:
                            compress_and_upload(content, gcs_bucket, destination_blob_name)
                        active_logger.info(f"Saved {'text' if dataset_type == 'search_results' else 'HTML'} for {identifier} to gs://{gcs_bucket}/{destination_blob_name}")
                    except Exception as e:
                        active_logger.error(f"Failed to upload for {identifier} ({url_for_gcs_path}): {str(e)}")
                        error_batch.set(db.collection(error_collection).document(identifier), {
                            "identifier": identifier,
                            "error": f"GCS upload failed: {str(e)}",
                            "stage": "store_html",
                            "retry_count": 0,
                            "original_item": item,
                            "timestamp": firestore.SERVER_TIMESTAMP
                        }, merge=True)
                        error_count += 1
                        results.append({"identifier": identifier, "url": url_for_gcs_path, "status": f"Flagged for retry (GCS upload): {str(e)}"})
                        publisher.publish(
                            publisher.topic_path(gcp_project_id, RETRY_TOPIC_NAME),
                            json.dumps({
                                "customer": customer_id,
                                "project": project_id,
                                "dataset_id": dataset_id,
                                "dataset_type": dataset_type,
                                "identifier": identifier,
                                "stage": "store_html",
                                "retry_count": 0
                            }).encode('utf-8')
                        )
                        continue

                    firestore_batch.update(doc_ref, {
                        "html_path" if dataset_type != "search_results" else "text_path": f"gs://{gcs_bucket}/{destination_blob_name}",
                        "html_stored_at": firestore.SERVER_TIMESTAMP,
                        "processing_stage": "store_html_success"
                    })
                    results.append({"identifier": identifier, "url": url_for_gcs_path, "gcs_path": f"gs://{gcs_bucket}/{destination_blob_name}", "status": "Success"})

                except Exception as e:
                    active_logger.error(f"Error processing item {identifier} (URL: {item_url_for_logging}): {str(e)}")
                    error_batch.set(db.collection(error_collection).document(identifier), {
                        "identifier": identifier,
                        "error": f"Item processing error: {str(e)}",
                        "stage": "store_html",
                        "retry_count": 0,
                        "original_item": item,
                        "timestamp": firestore.SERVER_TIMESTAMP
                    }, merge=True)
                    error_count += 1
                    results.append({"identifier": identifier, "url": item_url_for_logging, "status": f"Flagged for retry (processing): {str(e)}"})
                    publisher.publish(
                        publisher.topic_path(gcp_project_id, RETRY_TOPIC_NAME),
                        json.dumps({
                            "customer": customer_id,
                            "project": project_id,
                            "dataset_id": dataset_id,
                            "dataset_type": dataset_type,
                            "identifier": identifier,
                            "stage": "store_html",
                            "retry_count": 0
                        }).encode('utf-8')
                    )

            if len(firestore_batch._ops) > 0:
                try:
                    firestore_batch.commit()
                    active_logger.info(f"Committed Firestore updates for {len(firestore_batch._ops)} items.")
                except Exception as e:
                    active_logger.error(f"Error committing Firestore batch: {str(e)}")
                    for result_item in results[-len(firestore_batch._ops):]:
                        if result_item['status'] == "Success":
                            result_item['status'] = f"Firestore commit failed: {str(e)}"
                            error_batch.set(db.collection(error_collection).document(result_item['identifier']), {
                                "identifier": result_item['identifier'],
                                "error": f"Firestore commit failed: {str(e)}",
                                "stage": "store_html",
                                "retry_count": 0,
                                "original_item": item,
                                "timestamp": firestore.SERVER_TIMESTAMP
                            }, merge=True)
                            error_count += 1
                            publisher.publish(
                                publisher.topic_path(gcp_project_id, RETRY_TOPIC_NAME),
                                json.dumps({
                                    "customer": customer_id,
                                    "project": project_id,
                                    "dataset_id": dataset_id,
                                    "dataset_type": dataset_type,
                                    "identifier": result_item['identifier'],
                                    "stage": "store_html",
                                    "retry_count": 0
                                }).encode('utf-8')
                            )

            if error_count > 0:
                error_batch.commit()
                active_logger.info(f"Committed {error_count} error documents.")
                error_count = 0
                error_batch = db.batch()

            items_processed += len(items)
            if len(items) < items_to_fetch:
                active_logger.info("End of Apify dataset reached for the current offset range.")
                break

        success_count = sum(1 for r in results if r["status"] == "Success")
        active_logger.info(f"Processing summary: Attempted {items_processed} items from offset {offset}. Successfully stored {success_count} items.")

        report_id = f"store_html_{current_date_str}_{safe_project_id}_{safe_dataset_id_part}_{datetime.now().strftime('%H%M%S%f')}"
        report_ref = db.collection(f"{firestore_collection}_reports").document(report_id)
        report_ref.set({
            "report_type": "store_html_invocation",
            "customer_id": customer_id,
            "project_id": project_id,
            "dataset_id": dataset_id,
            "dataset_type": dataset_type,
            "triggering_offset": offset,
            "triggering_count": count,
            "items_fetched": items_processed,
            "results_summary": results[:50],
            "total_items": len(results),
            "total_successful": success_count,
            "processed_at": firestore.SERVER_TIMESTAMP
        })
        active_logger.info(f"Report generated: {report_id}")

        topic_path = publisher.topic_path(gcp_project_id, NEXT_STEP_TOPIC_NAME)
        next_step_payload = {
            "customer": customer_id,
            "project": project_id,
            "dataset_id": dataset_id,
            "dataset_type": dataset_type,
            "date": current_date_str,
            "store_html_offset": offset,
            "store_html_count": count,
            "status": "completed_chunk"
        }
        active_logger.info(f"Publishing to '{NEXT_STEP_TOPIC_NAME}': {next_step_payload}")
        future = publisher.publish(topic_path, json.dumps(next_step_payload).encode("utf-8"))
        try:
            msg_id = future.result(timeout=30)
            active_logger.info(f"Published message {msg_id} to '{NEXT_STEP_TOPIC_NAME}'.")
        except TimeoutError:
            active_logger.error(f"Publishing to '{NEXT_STEP_TOPIC_NAME}' timed out.")
            raise

        return {"status": "success", "items_processed": len(results), "successful_count": success_count, "report_id": report_id}

    except Exception as e:
        active_logger.error(f"Critical error in store_html: {str(e)}", exc_info=True)
        raise