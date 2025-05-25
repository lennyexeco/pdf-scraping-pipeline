import json
import base64
import logging
import os
import time
from google.cloud import firestore, storage, pubsub_v1
from src.common.utils import setup_logging
from src.common.config import load_customer_config, load_project_config
from src.common.helpers import get_mapped_field, sanitize_error_message
from datetime import datetime
import functions_framework
import pandas as pd

logger = logging.getLogger(__name__)
RETRY_TOPIC_NAME = "retry-pipeline"
MAX_GCS_RETRIES = 3
MAX_FIRESTORE_RETRIES = 3
RETRY_BACKOFF = 5  # seconds

def serialize_firestore_doc(data):
    """Convert Firestore document data to JSON-serializable format."""
    if isinstance(data, dict):
        return {k: serialize_firestore_doc(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [serialize_firestore_doc(item) for item in data]
    elif isinstance(data, datetime):
        return data.isoformat()
    return data

@functions_framework.cloud_event
def generate_reports(cloud_event):
    active_logger = logger
    try:
        pubsub_data = cloud_event.data.get("message", {}).get("data")
        if not pubsub_data:
            raise ValueError("No data in Pub/Sub message")

        data = json.loads(base64.b64decode(pubsub_data).decode('utf-8'))
        customer_id = data.get('customer')
        project_id = data.get('project')
        dataset_id = data.get('dataset_id')
        dataset_type = data.get('dataset_type', 'items')
        date = data.get('date', datetime.now().strftime('%Y%m%d'))

        if not all([customer_id, project_id, dataset_id, dataset_type, date]):
            raise ValueError("Missing required fields")

        active_logger = setup_logging(customer_id, project_id)
        active_logger.info(f"Generating report for dataset_id: {dataset_id}, type: {dataset_type}, date: {date}")

        customer_config = load_customer_config(customer_id)
        project_config = load_project_config(project_id)
        gcp_project_id = customer_config.get('gcp_project_id', os.environ.get("GCP_PROJECT"))
        if not gcp_project_id:
            raise ValueError("GCP Project ID not configured")

        bucket_name = project_config.get('gcs_bucket')
        firestore_collection = project_config.get('firestore_collection')
        search_results_collection = project_config.get('search_results_collection', f'{firestore_collection}_search_results')
        firestore_db_id = project_config.get('firestore_database_id', '(default)')
        report_config = project_config.get('report_config')
        field_mappings = project_config.get('field_mappings', {})
        error_collection = f"{firestore_collection}_errors"

        if not all([bucket_name, firestore_collection, report_config]):
            raise ValueError("Missing required configuration")

        storage_client = storage.Client(project=gcp_project_id)
        bucket = storage_client.bucket(bucket_name)
        db_options = {"project": gcp_project_id}
        if firestore_db_id != "(default)":
            db_options["database"] = firestore_db_id
        db = firestore.Client(**db_options)
        publisher = pubsub_v1.PublisherClient()

        # Determine report columns based on dataset_type
        report_columns = ['identifier'] + report_config.get('fields' if dataset_type == 'items' else 'search_results_fields', []) + ['errors']
        active_logger.info(f"Report columns for {dataset_type}: {report_columns}")

        collection = search_results_collection if dataset_type == 'search_results' else firestore_collection
        start_date = datetime.strptime(date, '%Y%m%d')
        end_date = start_date.replace(hour=23, minute=59, second=59)

        report_data = []
        for attempt in range(MAX_FIRESTORE_RETRIES):
            try:
                docs = db.collection(collection).where('dataset_id_source', '==', dataset_id).where('scrape_date', '>=', start_date).where('scrape_date', '<=', end_date).stream()
                break
            except Exception as e:
                active_logger.warning(f"Firestore query failed on attempt {attempt + 1}: {str(e)}")
                if attempt < MAX_FIRESTORE_RETRIES - 1:
                    time.sleep(RETRY_BACKOFF * (2 ** attempt))
                else:
                    active_logger.error(f"Max retries reached for Firestore query: {str(e)}")
                    raise

        for doc in docs:
            doc_data = doc.to_dict()
            doc_id = doc.id
            error_docs = db.collection(error_collection).where('identifier', '==', doc_id).stream()
            error_messages = [error_doc.to_dict().get('error', 'Unknown error') for error_doc in error_docs]
            errors = '; '.join(error_messages) if error_messages else 'None'

            row = {'identifier': doc_id, 'errors': errors}
            for field in report_columns:
                if field in ['identifier', 'errors']:
                    continue
                # Use get_mapped_field to resolve field value dynamically
                value = get_mapped_field(doc_data, field, field_mappings, logger_instance=active_logger)
                if value is None:
                    value = doc_data.get(field, 'Not Available')
                    active_logger.debug(f"Field {field} not found in mappings, using direct value: {value}")
                row[field] = str(value)
            report_data.append(row)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        safe_project_id = "".join(c if c.isalnum() else '_' for c in project_id)
        full_report_path = report_config.get('gcs_report_path', f"reports/{safe_project_id}/<date>").replace('<date>', date) + f"/report_{dataset_type}_{timestamp}.csv"
        blob = bucket.blob(full_report_path)

        if not report_data:
            active_logger.info(f"No documents found for dataset_id: {dataset_id}, date: {date}")
            empty_csv = ','.join(report_columns) + '\n'
            try:
                for attempt in range(MAX_GCS_RETRIES):
                    try:
                        blob.upload_from_string(empty_csv, content_type='text/csv')
                        active_logger.info(f"Saved empty report to gs://{bucket_name}/{full_report_path}")
                        break
                    except Exception as e:
                        active_logger.warning(f"GCS upload failed for {full_report_path} on attempt {attempt + 1}: {str(e)}")
                        if attempt < MAX_GCS_RETRIES - 1:
                            time.sleep(RETRY_BACKOFF * (2 ** attempt))
                        else:
                            raise Exception(f"Max retries reached for GCS upload: {str(e)}")
            except Exception as e:
                active_logger.error(f"Failed to upload empty report to GCS: {str(e)}")
                publisher.publish(
                    publisher.topic_path(gcp_project_id, RETRY_TOPIC_NAME),
                    json.dumps({
                        "customer": customer_id,
                        "project": project_id,
                        "dataset_id": dataset_id,
                        "dataset_type": dataset_type,
                        "identifier": f"report_{dataset_type}_{timestamp}",
                        "stage": "generate_reports",
                        "retry_count": 0,
                        "item_data_snapshot": serialize_firestore_doc(data)
                    }).encode('utf-8')
                )
                return {
                    'status': 'error',
                    'message': f"Empty report upload failed: {str(e)}",
                    'processed': 0,
                    'success': 0
                }
            return {'status': 'success', 'processed': 0, 'success': 0, 'report_path': f"gs://{bucket_name}/{full_report_path}"}

        df = pd.DataFrame(report_data, columns=report_columns)
        try:
            for attempt in range(MAX_GCS_RETRIES):
                try:
                    blob.upload_from_string(df.to_csv(index=False), content_type='text/csv')
                    active_logger.info(f"Saved report to gs://{bucket_name}/{full_report_path}")
                    break
                except Exception as e:
                    active_logger.warning(f"GCS upload failed for {full_report_path} on attempt {attempt + 1}: {str(e)}")
                    if attempt < MAX_GCS_RETRIES - 1:
                        time.sleep(RETRY_BACKOFF * (2 ** attempt))
                    else:
                        raise Exception(f"Max retries reached for GCS upload: {str(e)}")
        except Exception as e:
            active_logger.error(f"Failed to upload report to GCS: {str(e)}")
            publisher.publish(
                publisher.topic_path(gcp_project_id, RETRY_TOPIC_NAME),
                json.dumps({
                    "customer": customer_id,
                    "project": project_id,
                    "dataset_id": dataset_id,
                    "dataset_type": dataset_type,
                    "identifier": f"report_{dataset_type}_{timestamp}",
                    "stage": "generate_reports",
                    "retry_count": 0,
                    "item_data_snapshot": serialize_firestore_doc(data)
                }).encode('utf-8')
            )
            return {
                'status': 'error',
                'message': f"Report upload failed: {str(e)}",
                'processed': len(report_data),
                'success': 0
            }

        active_logger.info(f"Generated report with {len(report_data)} records at gs://{bucket_name}/{full_report_path}")
        return {
            'status': 'success',
            'processed': len(report_data),
            'success': len(report_data),
            'report_path': f"gs://{bucket_name}/{full_report_path}"
        }

    except Exception as e:
        active_logger.error(f"Critical error in generate_reports: {str(e)}", exc_info=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        publisher = pubsub_v1.PublisherClient()
        publisher.publish(
            publisher.topic_path(gcp_project_id, RETRY_TOPIC_NAME),
            json.dumps({
                "customer": customer_id if 'customer_id' in locals() else 'unknown',
                "project": project_id if 'project_id' in locals() else 'unknown',
                "dataset_id": dataset_id if 'dataset_id' in locals() else 'unknown',
                "dataset_type": dataset_type if 'dataset_type' in locals() else 'unknown',
                "identifier": f"report_{dataset_type}_{timestamp}" if 'dataset_type' in locals() else f"report_unknown_{timestamp}",
                "stage": "generate_reports",
                "retry_count": 0,
                "item_data_snapshot": serialize_firestore_doc(data) if 'data' in locals() else None
            }).encode('utf-8')
        )
        return {
            'status': 'error',
            'message': f"Critical error: {str(e)}",
            'processed': 0,
            'success': 0
        }