import json
import base64
import logging
from google.cloud import firestore, storage
from src.common.utils import setup_logging
from src.common.config import load_customer_config, load_project_config
from datetime import datetime
import functions_framework
import pandas as pd

logger = logging.getLogger(__name__)

@functions_framework.cloud_event
def generate_reports(cloud_event):
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
        report_config = project_config.get('report_config')

        if not all([bucket_name, firestore_collection, firestore_db_id, report_config]):
            raise ValueError("Missing required configuration fields")

        storage_client = storage.Client(project=customer_config['gcp_project_id'])
        bucket = storage_client.bucket(bucket_name)
        db = firestore.Client(project=customer_config['gcp_project_id'], database=firestore_db_id)

        collection = search_results_collection if dataset_type == 'search_results' else firestore_collection
        fields = report_config.get('search_results_fields' if dataset_type == 'search_results' else 'fields', [])
        report_path = report_config.get('gcs_report_path').replace('<project_id>', project_id).replace('<date>', date)

        docs = db.collection(collection).stream()
        report_data = []
        for doc in docs:
            doc_data = doc.to_dict()
            row = {field: doc_data.get(field, 'Not Available') for field in fields}
            row['identifier'] = doc.id
            report_data.append(row)

        if not report_data:
            active_logger.info("No data found for report generation.")
            return {'status': 'success', 'processed': 0, 'success': 0}

        df = pd.DataFrame(report_data)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        full_report_path = f"{report_path}/report_{dataset_type}_{timestamp}.csv"
        blob = bucket.blob(full_report_path)
        blob.upload_from_string(df.to_csv(index=False), content_type='text/csv')
        active_logger.info(f"Saved report to gs://{bucket_name}/{full_report_path}")

        report_ref = db.collection(f"{firestore_collection}_reports").document(f"report_{dataset_type}_{timestamp}")
        report_ref.set({
            "dataset_type": dataset_type,
            "report_path": f"gs://{bucket_name}/{full_report_path}",
            "total_records": len(report_data),
            "fields": fields,
            "processed_at": firestore.SERVER_TIMESTAMP
        })

        active_logger.info(f"Generated report with {len(report_data)} records.")
        return {'status': 'success', 'processed': len(report_data), 'success': len(report_data)}

    except Exception as e:
        active_logger.error(f"Error in generate_reports: {str(e)}", exc_info=True)
        raise