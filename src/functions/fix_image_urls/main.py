import json
import re
import gzip
import base64
from google.cloud import storage, firestore, pubsub_v1
from src.common.config import load_customer_config, load_project_config
from src.common.utils import generate_url_hash, logger
from datetime import datetime

def fix_image_urls(event, context):
    """Cloud Function to fix relative image URLs in HTML files."""
    try:
        # Decode Pub/Sub message
        pubsub_data = event.get('data')
        if not pubsub_data:
            logger.error("No data found in Pub/Sub event.")
            raise ValueError("No data in Pub/Sub message")

        data_str = base64.b64decode(pubsub_data).decode('utf-8')
        data = json.loads(data_str)
        logger.info(f"Decoded Pub/Sub message: {data}")

        customer_id = data.get('customer')
        project_id = data.get('project')
        date = data.get('date', datetime.now().strftime('%Y%m%d'))

        if not all([customer_id, project_id]):
            logger.error(f"Missing required fields (customer, project) in message: {data}")
            raise ValueError("Missing required fields in Pub/Sub message")

        # Load configurations
        customer_config = load_customer_config(customer_id)
        project_config = load_project_config(project_id)
        bucket_name = project_config.get('gcs_bucket')
        firestore_collection = project_config.get('firestore_collection')
        firestore_db_id = project_config.get('firestore_database_id')
        image_url_fix = project_config.get('image_url_fix')

        # Validate configuration
        required_fields = {
            'gcs_bucket': bucket_name,
            'firestore_collection': firestore_collection,
            'firestore_database_id': firestore_db_id,
            'image_url_fix': image_url_fix
        }
        missing_fields = [k for k, v in required_fields.items() if v is None]
        if missing_fields:
            logger.error(f"Missing required configuration fields: {missing_fields}")
            raise ValueError(f"Missing required configuration fields: {missing_fields}")

        base_url = image_url_fix.get('base_url', '').encode()
        regex_pattern = image_url_fix.get('regex_pattern')
        if not base_url or not regex_pattern:
            logger.error("Missing base_url or regex_pattern in image_url_fix configuration.")
            raise ValueError("Missing base_url or regex_pattern in image_url_fix")

        pattern = re.compile(regex_pattern.encode(), re.IGNORECASE | re.MULTILINE)
        pending_path = image_url_fix.get('gcs_pending_path', 'pending/<project_id>/<date>').replace('<project_id>', project_id).replace('<date>', date)
        fixed_path_template = image_url_fix.get('gcs_fixed_path', 'fixed/<project_id>/<date>').replace('<project_id>', project_id).replace('<date>', date)

        # Initialize clients
        storage_client = storage.Client(project=customer_config['gcp_project_id'])
        bucket = storage_client.bucket(bucket_name)
        db = firestore.Client(project=customer_config['gcp_project_id'], database=firestore_db_id)
        logger.info(f"Initialized clients for bucket '{bucket_name}', Firestore collection '{firestore_collection}', database '{firestore_db_id}'")

        # List HTML files in GCS
        blobs = bucket.list_blobs(prefix=pending_path)
        results = []
        batch_size = 10

        for blob in blobs:
            if not blob.name.endswith('.html.gz'):
                continue

            filename = blob.name.split('/')[-1].replace('.html.gz', '')
            ecli = filename

            doc_ref = db.collection(firestore_collection).document(ecli)
            doc = doc_ref.get()
            if doc.exists and doc.to_dict().get('fixed_html_path'):
                logger.info(f"Skipping {blob.name}: Already fixed at {doc.to_dict()['fixed_html_path']}")
                results.append({
                    'filename': blob.name,
                    'ecli': ecli,
                    'images_fixed': doc.to_dict().get('images_fixed', 0),
                    'status': 'Skipped (already fixed)'
                })
                continue

            try:
                content = gzip.decompress(blob.download_as_bytes())
            except Exception as e:
                logger.error(f"Failed to decompress {blob.name}: {str(e)}")
                results.append({
                    'filename': blob.name,
                    'ecli': ecli,
                    'images_fixed': 0,
                    'status': f'Error: Failed to decompress ({str(e)})'
                })
                continue

            original_count = len(pattern.findall(content))
            if original_count == 0:
                results.append({
                    'filename': blob.name,
                    'ecli': ecli,
                    'images_fixed': 0,
                    'status': 'No images to fix'
                })
                continue

            try:
                fixed_content = pattern.sub(rb'\1' + base_url + rb'\2\3', content)
            except Exception as e:
                logger.error(f"Failed to fix URLs in {blob.name}: {str(e)}")
                results.append({
                    'filename': blob.name,
                    'ecli': ecli,
                    'images_fixed': 0,
                    'status': f'Error: Failed to fix URLs ({str(e)})'
                })
                continue

            fixed_path = f"{fixed_path_template}/{ecli}.html.gz"
            fixed_blob = bucket.blob(fixed_path)
            try:
                fixed_blob.upload_from_string(gzip.compress(fixed_content), content_type='application/gzip')
                logger.info(f"Saved fixed HTML to gs://{bucket_name}/{fixed_path}")
            except Exception as e:
                logger.error(f"Failed to upload fixed HTML to {fixed_path}: {str(e)}")
                results.append({
                    'filename': blob.name,
                    'ecli': ecli,
                    'images_fixed': 0,
                    'status': f'Error: Failed to upload ({str(e)})'
                })
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
                    'ecli': ecli,
                    'images_fixed': original_count,
                    'status': 'Success'
                })
            except Exception as e:
                logger.error(f"Failed to update Firestore for {ecli}: {str(e)}")
                results.append({
                    'filename': blob.name,
                    'ecli': ecli,
                    'images_fixed': original_count,
                    'status': f'Error: Failed to update Firestore ({str(e)})'
                })

            if len(results) >= batch_size:
                save_batch_results(results, bucket, project_id, date, firestore_collection, firestore_db_id)
                results = []

        if results:
            save_batch_results(results, bucket, project_id, date, firestore_collection, firestore_db_id)

        success_count = sum(1 for r in results if r['status'] == 'Success')
        logger.info(f"Processed {len(results)} files: {success_count} successful, {len(results) - success_count} failed")

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(customer_config['gcp_project_id'], 'generate-xml')
        next_message = {
            'customer': customer_id,
            'project': project_id,
            'date': date
        }
        logger.info(f"Attempting to publish to 'generate-xml' with payload: {next_message}")
        future = publisher.publish(topic_path, json.dumps(next_message).encode('utf-8'))
        try:
            msg_id = future.result(timeout=30)
            logger.info(f"Successfully published message {msg_id} to 'generate-xml'.")
        except TimeoutError:
            logger.error("Publishing to 'generate-xml' timed out.", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Failed to publish to 'generate-xml': {str(e)}", exc_info=True)
            raise

        return {
            'status': 'success',
            'processed': len(results),
            'success': success_count
        }

    except Exception as e:
        logger.error(f"Unexpected error in fix_image_urls: {str(e)}", exc_info=True)
        raise

def save_batch_results(results, bucket, project_id, date, firestore_collection, firestore_db_id):
    """Save processing results to Firestore and GCS."""
    import pandas as pd
    from datetime import datetime

    db = firestore.Client(project='execo-harvey', database=firestore_db_id)
    for result in results:
        doc_ref = db.collection(f"{firestore_collection}_reports").document(generate_url_hash(result['filename']))
        doc_ref.set(result)

    df = pd.DataFrame(results)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_path = f"reports/{project_id}/{date}/image_fix_report_{timestamp}.csv"
    blob = bucket.blob(report_path)
    blob.upload_from_string(df.to_csv(index=False), content_type='text/csv')
    logger.info(f"Saved report to gs://{bucket.name}/{report_path}")