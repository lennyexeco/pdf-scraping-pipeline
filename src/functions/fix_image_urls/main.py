import json
import base64
import logging
import re
import gzip
from google.cloud import firestore, pubsub_v1, storage
from src.common.utils import generate_url_hash, compress_and_upload, setup_logging
from src.common.config import load_customer_config, load_project_config
from datetime import datetime
import functions_framework
import pandas as pd

logger = logging.getLogger(__name__)
RETRY_TOPIC_NAME = "retry-pipeline"

def save_batch_results(results, bucket, project_id, date, firestore_collection, firestore_db_id, logger_instance):
    db = firestore.Client(project='execo-harvey', database=firestore_db_id)
    for result in results:
        doc_ref = db.collection(f"{firestore_collection}_reports").document(generate_url_hash(result['filename']))
        doc_ref.set(result)
    df = pd.DataFrame(results)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_path = f"reports/{project_id}/{date}/image_fix_report_{timestamp}.csv"
    blob = bucket.blob(report_path)
    blob.upload_from_string(df.to_csv(index=False), content_type='text/csv')
    logger_instance.info(f"Saved report to gs://{bucket.name}/{report_path}")

@functions_framework.cloud_event
def fix_image_urls(cloud_event):
    active_logger = logger
    try:
        pubsub_data = cloud_event.data.get("message", {}).get("data")
        if not pubsub_data:
            raise ValueError("No data in Pub/Sub message")

        data = json.loads(base64.b64decode(pubsub_data).decode('utf-8'))
        active_logger.info(f"Decoded Pub/Sub message: {data}")

        customer_id = data.get('customer')
        project_id = data.get('project')
        dataset_type = data.get('dataset_type', 'items')
        date = data.get('date', datetime.now().strftime('%Y%m%d'))

        if not all([customer_id, project_id, dataset_type]):
            raise ValueError("Missing required fields in Pub/Sub message")

        active_logger = setup_logging(customer_id, project_id)
        customer_config = load_customer_config(customer_id)
        project_config = load_project_config(project_id)
        bucket_name = project_config.get('gcs_bucket')
        firestore_collection = project_config.get('firestore_collection')
        search_results_collection = project_config.get('search_results_collection', f'{firestore_collection}_search_results')
        firestore_db_id = project_config.get('firestore_database_id')
        image_url_fix = project_config.get('image_url_fix')
        error_collection = f"{firestore_collection}_errors"

        if not all([bucket_name, firestore_collection, firestore_db_id]):
            raise ValueError("Missing required configuration fields")

        if dataset_type == "search_results":
            if project_config.get("process_search_results_images", False):
                active_logger.info("Processing image URLs for search_results")
            else:
                active_logger.info("Skipping image URL fixing for search_results")
                publisher = pubsub_v1.PublisherClient()
                topic_path = publisher.topic_path(customer_config['gcp_project_id'], 'generate-xml')
                next_message = {
                    'customer': customer_id,
                    'project': project_id,
                    'dataset_type': dataset_type,
                    'date': date
                }
                active_logger.info(f"Publishing to 'generate-xml': {next_message}")
                future = publisher.publish(topic_path, json.dumps(next_message).encode('utf-8'))
                try:
                    msg_id = future.result(timeout=30)
                    active_logger.info(f"Published message {msg_id} to 'generate-xml'.")
                except TimeoutError:
                    active_logger.error("Publishing timed out.")
                    raise
                return {'status': 'success', 'processed': 0, 'success': 0}

        base_url = image_url_fix.get('base_url', '').encode()
        regex_pattern = image_url_fix.get('regex_pattern')
        if not base_url or not regex_pattern:
            raise ValueError("Missing base_url or regex_pattern")

        pattern = re.compile(regex_pattern.encode(), re.IGNORECASE | re.MULTILINE)
        pending_path = image_url_fix.get('gcs_pending_path').replace('<project_id>', project_id).replace('<date>', date)
        fixed_path_template = image_url_fix.get('gcs_fixed_path').replace('<project_id>', project_id).replace('<date>', date)

        storage_client = storage.Client(project=customer_config['gcp_project_id'])
        bucket = storage_client.bucket(bucket_name)
        db = firestore.Client(project=customer_config['gcp_project_id'], database=firestore_db_id)
        publisher = pubsub_v1.PublisherClient()

        blobs = bucket.list_blobs(prefix=pending_path)
        results = []
        error_batch = db.batch()
        batch_size = 10

        for blob in blobs:
            if not blob.name.endswith('.html.gz'):
                continue

            identifier = blob.name.split('/')[-1].replace('.html.gz', '')
            collection = firestore_collection

            doc_ref = db.collection(collection).document(identifier)
            doc = doc_ref.get()
            if doc.exists and doc.to_dict().get('fixed_html_path'):
                active_logger.info(f"Skipping {blob.name}: Already fixed")
                results.append({
                    'filename': blob.name,
                    'identifier': identifier,
                    'images_fixed': doc.to_dict().get('images_fixed', 0),
                    'status': 'Skipped (already fixed)'
                })
                continue

            try:
                content = gzip.decompress(blob.download_as_bytes())
            except Exception as e:
                active_logger.error(f"Failed to decompress {blob.name}: {str(e)}")
                error_batch.set(db.collection(error_collection).document(identifier), {
                    "identifier": identifier,
                    "error": f"Decompression failed: {str(e)}",
                    "stage": "fix_image_urls",
                    "retry_count": 0,
                    "original_blob": blob.name,
                    "timestamp": firestore.SERVER_TIMESTAMP
                })
                results.append({
                    'filename': blob.name,
                    'identifier': identifier,
                    'images_fixed': 0,
                    'status': f"Flagged for retry (decompression): {str(e)}"
                })
                publisher.publish(publisher.topic_path(customer_config['gcp_project_id'], RETRY_TOPIC_NAME), json.dumps({
                    "customer": customer_id,
                    "project": project_id,
                    "dataset_id": data.get("dataset_id"),
                    "dataset_type": dataset_type,
                    "identifier": identifier,
                    "stage": "fix_image_urls",
                    "retry_count": 0
                }).encode('utf-8'))
                continue

            original_count = len(pattern.findall(content))
            if original_count == 0:
                fixed_path = blob.name
                try:
                    doc_ref.update({
                        'fixed_html_path': f"gs://{bucket_name}/{fixed_path}",
                        'images_fixed': 0,
                        'fix_status': 'Success',
                        'fixed_at': firestore.SERVER_TIMESTAMP
                    })
                    results.append({
                        'filename': blob.name,
                        'identifier': identifier,
                        'images_fixed': 0,
                        'status': 'Success'
                    })
                    active_logger.info(f"No images to fix for {blob.name}, marked as Success")
                except Exception as e:
                    active_logger.error(f"Failed to update Firestore for {identifier} (no images): {str(e)}")
                    error_batch.set(db.collection(error_collection).document(identifier), {
                        "identifier": identifier,
                        "error": f"Firestore update failed (no images): {str(e)}",
                        "stage": "fix_image_urls",
                        "retry_count": 0,
                        "original_blob": blob.name,
                        "timestamp": firestore.SERVER_TIMESTAMP
                    })
                    results.append({
                        'filename': blob.name,
                        'identifier': identifier,
                        'images_fixed': 0,
                        'status': f"Flagged for retry (Firestore update, no images): {str(e)}"
                    })
                    publisher.publish(publisher.topic_path(customer_config['gcp_project_id'], RETRY_TOPIC_NAME), json.dumps({
                        "customer": customer_id,
                        "project": project_id,
                        "dataset_id": data.get("dataset_id"),
                        "dataset_type": dataset_type,
                        "identifier": identifier,
                        "stage": "fix_image_urls",
                        "retry_count": 0
                    }).encode('utf-8'))
                continue

            try:
                fixed_content = pattern.sub(rb'\1' + base_url + rb'\2\3', content)
            except Exception as e:
                active_logger.error(f"Failed to fix URLs in {blob.name}: {str(e)}")
                error_batch.set(db.collection(error_collection).document(identifier), {
                    "identifier": identifier,
                    "error": f"URL fix failed: {str(e)}",
                    "stage": "fix_image_urls",
                    "retry_count": 0,
                    "original_blob": blob.name,
                    "timestamp": firestore.SERVER_TIMESTAMP
                })
                results.append({
                    'filename': blob.name,
                    'identifier': identifier,
                    'images_fixed': 0,
                    'status': f"Flagged for retry (URL fix): {str(e)}"
                })
                publisher.publish(publisher.topic_path(customer_config['gcp_project_id'], RETRY_TOPIC_NAME), json.dumps({
                    "customer": customer_id,
                    "project": project_id,
                    "dataset_id": data.get("dataset_id"),
                    "dataset_type": dataset_type,
                    "identifier": identifier,
                    "stage": "fix_image_urls",
                    "retry_count": 0
                }).encode('utf-8'))
                continue

            fixed_path = f"{fixed_path_template}/{identifier}.html.gz"
            fixed_blob = bucket.blob(fixed_path)
            try:
                fixed_blob.upload_from_string(gzip.compress(fixed_content), content_type='application/gzip')
                active_logger.info(f"Saved fixed HTML to gs://{bucket_name}/{fixed_path}")
            except Exception as e:
                active_logger.error(f"Failed to upload fixed HTML: {str(e)}")
                error_batch.set(db.collection(error_collection).document(identifier), {
                    "identifier": identifier,
                    "error": f"Upload failed: {str(e)}",
                    "stage": "fix_image_urls",
                    "retry_count": 0,
                    "original_blob": blob.name,
                    "timestamp": firestore.SERVER_TIMESTAMP
                })
                results.append({
                    'filename': blob.name,
                    'identifier': identifier,
                    'images_fixed': 0,
                    'status': f"Flagged for retry (upload): {str(e)}"
                })
                publisher.publish(publisher.topic_path(customer_config['gcp_project_id'], RETRY_TOPIC_NAME), json.dumps({
                    "customer": customer_id,
                    "project": project_id,
                    "dataset_id": data.get("dataset_id"),
                    "dataset_type": dataset_type,
                    "identifier": identifier,
                    "stage": "fix_image_urls",
                    "retry_count": 0
                }).encode('utf-8'))
                continue

            try:
                doc_ref.update({
                    'fixed_html_path': f"gs://{bucket_name}/{fixed_path}",
                    'images_fixed': original_count,
                    'fix_status': 'Success',
                    'fixed_at': firestore.SERVER_TIMESTAMP
                })
                results.append({
                    'filename': blob.name,
                    'identifier': identifier,
                    'images_fixed': original_count,
                    'status': 'Success'
                })
            except Exception as e:
                active_logger.error(f"Failed to update Firestore for {identifier}: {str(e)}")
                error_batch.set(db.collection(error_collection).document(identifier), {
                    "identifier": identifier,
                    "error": f"Firestore update failed: {str(e)}",
                    "stage": "fix_image_urls",
                    "retry_count": 0,
                    "original_blob": blob.name,
                    "timestamp": firestore.SERVER_TIMESTAMP
                })
                results.append({
                    'filename': blob.name,
                    'identifier': identifier,
                    'images_fixed': original_count,
                    'status': f"Flagged for retry (Firestore update): {str(e)}"
                })
                publisher.publish(publisher.topic_path(customer_config['gcp_project_id'], RETRY_TOPIC_NAME), json.dumps({
                    "customer": customer_id,
                    "project": project_id,
                    "dataset_id": data.get("dataset_id"),
                    "dataset_type": dataset_type,
                    "identifier": identifier,
                    "stage": "fix_image_urls",
                    "retry_count": 0
                }).encode('utf-8'))

            if len(results) >= batch_size:
                save_batch_results(results, bucket, project_id, date, firestore_collection, firestore_db_id, active_logger)
                results = []

        if results:
            save_batch_results(results, bucket, project_id, date, firestore_collection, firestore_db_id, active_logger)

        if error_batch._operations:
            error_batch.commit()
            active_logger.info(f"Flagged {len(error_batch._operations)} errors for retry.")

        success_count = sum(1 for r in results if r['status'] == 'Success')
        active_logger.info(f"Processed {len(results)} files: {success_count} successful")

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(customer_config['gcp_project_id'], 'generate-xml')
        next_message = {
            'customer': customer_id,
            'project': project_id,
            'dataset_type': dataset_type,
            'date': date
        }
        active_logger.info(f"Publishing to 'generate-xml': {next_message}")
        future = publisher.publish(topic_path, json.dumps(next_message).encode('utf-8'))
        try:
            msg_id = future.result(timeout=30)
            active_logger.info(f"Published message {msg_id} to 'generate-xml'.")
        except TimeoutError:
            active_logger.error("Publishing timed out.")
            raise

        return {'status': 'success', 'processed': len(results), 'success': success_count}

    except Exception as e:
        active_logger.error(f"Error in fix_image_urls: {str(e)}", exc_info=True)
        raise