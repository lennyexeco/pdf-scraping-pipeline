import json
import pandas as pd
from google.cloud import storage, firestore
from src.common.config import load_customer_config, load_project_config
from src.common.utils import setup_logging
from datetime import datetime
import functions_framework

@functions_framework.cloud_event
def generate_reports(event, context):
    """Cloud Function to generate processing reports."""
    data = json.loads(event['data'].decode('utf-8'))
    customer_id = data['customer']
    project_id = data['project']
    date = data.get('date', datetime.now().strftime('%Y%m%d'))

    # Initialize logger
    logger = setup_logging(customer_id, project_id)

    customer_config = load_customer_config(customer_id)
    project_config = load_project_config(project_id)
    bucket_name = project_config['gcs_bucket']
    firestore_collection = project_config['firestore_collection']

    db = firestore.Client(project=customer_config['gcp_project_id'], database=customer_config['firestore_database'])
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Fetch results from Firestore
    docs = db.collection(firestore_collection).stream()
    results = []
    for doc in docs:
        data = doc.to_dict()
        results.append({
            'url': data.get('url', ''),
            'title': data.get('title', ''),
            'description': data.get('description', ''),
            'ProcessingStatus': data.get('fix_status', 'Pending'),
            'HTMLPath': data.get('fixed_html_path', ''),
            'XMLPath': data.get('xml_path', '')
        })

    # Generate reports
    success_rows = [r for r in results if r['ProcessingStatus'] == 'Success']
    failed_rows = [r for r in results if r['ProcessingStatus'] != 'Success']
    all_rows = results

    for df, report_type in [
        (pd.DataFrame(success_rows), 'success'),
        (pd.DataFrame(failed_rows), 'failed'),
        (pd.DataFrame(all_rows), 'all')
    ]:
        if not df.empty:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"reports/{project_id}/{date}/report_{report_type}_{timestamp}.csv"
            blob = bucket.blob(filename)
            blob.upload_from_string(df.to_csv(index=False), content_type="text/csv")
            logger.info(f"Saved {report_type} report to gs://{bucket_name}/{filename}")