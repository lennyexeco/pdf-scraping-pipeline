import json
import re
from google.cloud import storage, firestore, pubsub_v1
from src.common.config import load_customer_config, load_project_config
from src.common.utils import generate_url_hash, logger
from datetime import datetime

def fix_image_urls(event, context):
    """Cloud Function to fix relative image URLs in HTML files."""
    data = json.loads(event['data'].decode('utf-8'))
    customer_id = data['customer']
    project_id = data['project']
    date = datetime.now().strftime('%Y%m%d')

    # Load configurations
    customer_config = load_customer_config(customer_id)
    project_config = load_project_config(project_id)
    bucket_name = project_config['gcs_bucket']
    firestore_collection = project_config['firestore_collection']
    base_url = project_config['image_url_fix']['base_url'].encode()
    pattern = re.compile(project_config['image_url_fix']['regex_pattern'].encode(), re.IGNORECASE | re.MULTILINE)

    # Initialize clients
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    db = firestore.Client(project=customer_config['gcp_project_id'], database=customer_config['firestore_database'])

    # List HTML files in GCS
    blobs = bucket.list_blobs(prefix=f"pending/{project_id}/{date}/")
    results = []
    batch_size = 10  # Smaller batch for serverless

    for blob in blobs:
        if not blob.name.endswith('.html.gz'):
            continue

        # Download and decompress
        content = gzip.decompress(blob.download_as_bytes())

        # Count and fix image URLs
        original_count = len(pattern.findall(content))
        if original_count == 0:
            results.append({
                'filename': blob.name,
                'images_fixed': 0,
                'status': 'No images to fix'
            })
            continue

        fixed_content = pattern.sub(rb'\1' + base_url + rb'\2\3', content)

        # Save fixed HTML to GCS
        fixed_path = blob.name.replace('pending/', 'fixed/')
        fixed_blob = bucket.blob(fixed_path)
        fixed_blob.upload_from_string(gzip.compress(fixed_content), content_type='text/html')
        logger.info(f"Saved fixed HTML to gs://{bucket_name}/{fixed_path}")

        # Update Firestore
        url_hash = generate_url_hash(blob.name)
        doc_ref = db.collection(firestore_collection).document(url_hash)
        doc_ref.update({
            'fixed_html_path': f"gs://{bucket_name}/{fixed_path}",
            'images_fixed': original_count,
            'fix_status': 'Success'
        })

        results.append({
            'filename': blob.name,
            'images_fixed': original_count,
            'status': 'Success'
        })

        # Limit batch size for serverless
        if len(results) >= batch_size:
            save_batch_results(results, bucket, project_id, date)
            results = []

    # Save remaining results
    if results:
        save_batch_results(results, bucket, project_id, date)

    # Trigger next step
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(customer_config['gcp_project_id'], 'generate-xml')
    publisher.publish(topic_path, json.dumps({
        'customer': customer_id,
        'project': project_id
    }).encode('utf-8'))

def save_batch_results(results, bucket, project_id, date):
    """Save processing results to Firestore and GCS."""
    import pandas as pd
    from datetime import datetime

    # Save to Firestore
    db = firestore.Client()
    for result in results:
        doc_ref = db.collection(f"harvey/{project_id}/reports").document(generate_url_hash(result['filename']))
        doc_ref.set(result)

    # Save CSV report to GCS
    df = pd.DataFrame(results)
    report_path = f"reports/{project_id}/{date}/image_fix_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    blob = bucket.blob(report_path)
    blob.upload_from_string(df.to_csv(index=False), content_type='text/csv')
    logger.info(f"Saved report to gs://{bucket.name}/{report_path}")