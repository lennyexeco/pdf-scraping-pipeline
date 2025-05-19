import functions_framework
import base64
import json
import os
import logging
from google.cloud import pubsub_v1, storage, firestore
from apify_client import ApifyClient
from src.common.config import load_customer_config, load_project_config, get_secret
from src.common.utils import compress_and_upload, generate_url_hash, setup_logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
module_logger = logging.getLogger(__name__)

@functions_framework.cloud_event
def store_html(cloud_event):
    active_logger = module_logger
    try:
        pubsub_data_encoded = cloud_event.data.get("message", {}).get("data")
        if not pubsub_data_encoded:
            if isinstance(cloud_event.data, dict) and "data" in cloud_event.data and isinstance(cloud_event.data["data"], str):
                pubsub_data_encoded = cloud_event.data["data"]
            elif isinstance(cloud_event.data, dict) and "customer" in cloud_event.data:
                data = cloud_event.data
                module_logger.info("Interpreting cloud_event.data directly as JSON payload.")
            else:
                module_logger.error(f"Could not find 'data' in cloud_event.data.message or cloud_event.data. Received data: {cloud_event.data}")
                raise ValueError("Pub/Sub message data not found in expected format.")

        if pubsub_data_encoded and 'data' not in locals():
            data = json.loads(base64.b64decode(pubsub_data_encoded).decode("utf-8"))
            module_logger.info(f"Successfully decoded Pub/Sub message: {data}")

        customer_id = data.get("customer")
        project_id = data.get("project")
        dataset_id = data.get("dataset_id")
        offset = int(data.get("offset", 0))
        count = int(data.get("count", 50))

        if not all([customer_id, project_id, dataset_id]):
            module_logger.error(f"Missing one or more required fields (customer, project, dataset_id) in message: {data}")
            raise ValueError("Missing required fields in Pub/Sub message")

        try:
            active_logger = setup_logging(customer_id, project_id)
            active_logger.info(f"Successfully set up custom logger for customer '{customer_id}', project '{project_id}'.")
        except Exception as su_err:
            module_logger.error(f"Failed to initialize custom logger via setup_logging for customer '{customer_id}', project '{project_id}': {su_err}", exc_info=True)

        active_logger.info(f"Starting store_html for customer: {customer_id}, project: {project_id}, dataset_id: {dataset_id}, offset: {offset}, count: {count}")

        customer_config = load_customer_config(customer_id)
        project_config = load_project_config(project_id)

        gcs_bucket = project_config["gcs_bucket"]
        firestore_collection = project_config["firestore_collection"]
        firestore_db_id = project_config.get("firestore_database_id")
        batch_size = int(project_config.get("batch_size", 10))

        gcp_project_id_for_clients = customer_config.get("gcp_project_id", os.environ.get("GCP_PROJECT"))
        if not gcp_project_id_for_clients:
            active_logger.error("GCP Project ID not found in customer config or environment variable GCP_PROJECT.")
            raise ValueError("GCP Project ID for clients is not configured.")

        apify_key = get_secret(customer_config["apify_api_key_secret"], gcp_project_id_for_clients)
        apify_client = ApifyClient(apify_key)
        storage_client = storage.Client(project=gcp_project_id_for_clients)
        bucket = storage_client.bucket(gcs_bucket)

        db_options = {"project": gcp_project_id_for_clients}
        if firestore_db_id:
            db_options["database"] = firestore_db_id
        db = firestore.Client(**db_options)
        active_logger.info(f"Initialized clients. Firestore project '{gcp_project_id_for_clients}', database '{firestore_db_id or '(default)'}'")

        results = []
        current_date_str = data.get("date", datetime.now().strftime('%Y%m%d'))
        items_processed = 0

        while items_processed < count:
            items_to_fetch = min(batch_size, count - items_processed)
            active_logger.info(f"Fetching Apify data for store_html. Dataset: {dataset_id}, Offset: {offset + items_processed}, Limit: {items_to_fetch}")
            response = apify_client.dataset(dataset_id).list_items(
                offset=offset + items_processed,
                limit=items_to_fetch
            )
            items = response.items
            active_logger.info(f"Fetched {len(items)} items from Apify for store_html.")

            if not items:
                active_logger.info("No more items found in Apify dataset for store_html.")
                break

            for item in items:
                url = item.get("url", "")
                content = item.get("content", "")
                ecli = item.get("ecliId", item.get("uniqueId", ""))
                filename_base = item.get("title", "untitled").replace(" ", "_")[:250]

                if not ecli:
                    if not url:
                        active_logger.warning(f"Skipping item (title: {filename_base}) due to missing ECLI and URL.")
                        results.append({"ecli": "UNKNOWN_NO_ID_NO_URL", "url": "", "status": "Skipped - No ECLI or URL"})
                        continue
                    ecli = generate_url_hash(url)
                    active_logger.warning(f"No ECLI for {url}, using URL hash: {ecli}")

                if not content:
                    active_logger.warning(f"No content for {url} (ECLI: {ecli}), skipping HTML storage.")
                    results.append({"ecli": ecli, "url": url, "status": "No content, skipped"})
                    continue

                destination = f"pending/{project_id}/{current_date_str}/{ecli}.html.gz"
                try:
                    compress_and_upload(content, gcs_bucket, destination)  # Removed storage_client_to_use
                    active_logger.info(f"Saved HTML for ECLI {ecli} to gs://{gcs_bucket}/{destination}")
                except Exception as e_gcs:
                    active_logger.error(f"Failed to upload HTML for {ecli} ({url}) to GCS: {str(e_gcs)}")
                    results.append({"ecli": ecli, "url": url, "status": f"GCS Upload Error: {str(e_gcs)}"})
                    continue

                try:
                    doc_ref = db.collection(firestore_collection).document(ecli)
                    doc_ref.update({
                        "html_path": f"gs://{gcs_bucket}/{destination}",
                        "html_stored_at": firestore.SERVER_TIMESTAMP
                    })
                    results.append({"ecli": ecli, "url": url, "status": "Success"})
                    active_logger.info(f"Updated Firestore for ECLI {ecli} with HTML path.")
                except Exception as e_fs:
                    active_logger.error(f"Failed to update Firestore for {ecli} after GCS upload: {str(e_fs)}")
                    results.append({"ecli": ecli, "url": url, "status": f"Firestore Update Error: {str(e_fs)}"})

            items_processed += len(items)
            if len(items) < items_to_fetch:
                active_logger.info("Fetched fewer items than batch size from Apify, assuming end of dataset.")
                break

        success_count = sum(1 for r in results if r["status"] == "Success")
        active_logger.info(f"Processed {len(results)} items for HTML storage: {success_count} successful, {len(results) - success_count} failed/skipped.")

        report_ref = db.collection(f"{firestore_collection}_reports").document(f"store_html_{current_date_str}_{dataset_id[:10]}_{datetime.now().strftime('%H%M%S')}")
        report_ref.set({
            "dataset_id": dataset_id,
            "results_summary": results,
            "total_attempted": len(results),
            "total_successful_storage": success_count,
            "processed_at": firestore.SERVER_TIMESTAMP
        })
        active_logger.info(f"Saved store_html processing report to Firestore.")

        next_step_topic_name = "fix-image-urls"
        publisher_next = pubsub_v1.PublisherClient()
        topic_path_next_step = publisher_next.topic_path(gcp_project_id_for_clients, next_step_topic_name)

        next_step_payload = {
            "customer": customer_id,
            "project": project_id,
            "dataset_id": dataset_id,
            "date": current_date_str
        }
        active_logger.info(f"Attempting to publish to '{next_step_topic_name}' with payload: {next_step_payload}")
        future_next = publisher_next.publish(topic_path_next_step, json.dumps(next_step_payload).encode("utf-8"))
        try:
            msg_id_next = future_next.result(timeout=30)
            active_logger.info(f"Successfully published message {msg_id_next} to '{next_step_topic_name}'.")
        except TimeoutError:
            active_logger.error(f"Publishing to '{next_step_topic_name}' timed out.", exc_info=True)
            raise
        except Exception as pub_err:
            active_logger.error(f"Failed to publish to '{next_step_topic_name}': {pub_err}", exc_info=True)
            raise

        return {"status": "success", "processed_items_summary": len(results), "successful_storage": success_count}

    except Exception as e:
        active_logger.error(f"Unexpected error in store_html: {str(e)}", exc_info=True)
        raise