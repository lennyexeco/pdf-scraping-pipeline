import json
import pandas as pd
import base64
import logging
from google.cloud import storage, firestore
from src.common.config import load_customer_config, load_project_config
from src.common.utils import setup_logging
from datetime import datetime
import functions_framework

# Fallback logger for early errors
fallback_logger = logging.getLogger('fallback_generate_reports')
fallback_logger.setLevel(logging.ERROR)
if not fallback_logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
    fallback_logger.addHandler(handler)

@functions_framework.cloud_event
def generate_reports(cloud_event):
    """Cloud Function to generate processing reports."""
    logger = fallback_logger  # Default to fallback
    try:
        # Decode Pub/Sub message
        pubsub_data = cloud_event.data.get("message", {}).get("data")
        if not pubsub_data:
            raise ValueError(f"No message data in CloudEvent: {cloud_event.data}")

        data_str = base64.b64decode(pubsub_data).decode('utf-8')
        data = json.loads(data_str)

        customer_id = data.get('customer')
        project_id = data.get('project')
        date = data.get('date', datetime.now().strftime('%Y%m%d'))

        if not all([customer_id, project_id]):
            raise ValueError(f"Missing required fields (customer, project) in message: {data}")

        # Initialize custom logger
        try:
            logger = setup_logging(customer_id, project_id)
            logger.info(f"Decoded Pub/Sub message: {data}")
        except Exception as e:
            fallback_logger.error(f"Failed to initialize logger: {str(e)}")
            # Continue with fallback logger

        # Load configurations
        customer_config = load_customer_config(customer_id)
        project_config = load_project_config(project_id)
        bucket_name = project_config.get('gcs_bucket')
        firestore_collection = project_config.get('firestore_collection')
        firestore_db_id = project_config.get('firestore_database_id')
        report_config = project_config.get('report_config', {})

        # Validate configuration
        required_fields = {
            'gcs_bucket': bucket_name,
            'firestore_collection': firestore_collection,
            'firestore_database_id': firestore_db_id
        }
        missing_fields = [k for k, v in required_fields.items() if v is None]
        if missing_fields:
            logger.error(f"Missing required configuration fields: {missing_fields}")
            raise ValueError(f"Missing required configuration fields: {missing_fields}")

        report_fields = report_config.get('fields', project_config.get('required_fields', []) + ['html_path', 'fixed_html_path', 'xml_path', 'xml_status', 'images_fixed', 'xml_generated_at'])
        gcs_report_path = report_config.get('gcs_report_path', 'reports/<project_id>/<date>').replace('<project_id>', project_id).replace('<date>', date)

        # Initialize clients
        db = firestore.Client(project=customer_config['gcp_project_id'], database=firestore_db_id)
        storage_client = storage.Client(project=customer_config['gcp_project_id'])
        bucket = storage_client.bucket(bucket_name)
        logger.info(f"Initialized clients for bucket '{bucket_name}', Firestore collection '{firestore_collection}', database '{firestore_db_id}'")

        # Fetch results from Firestore, scoped to date
        results = []
        start_date = datetime.strptime(date, '%Y%m%d')
        query = db.collection(firestore_collection).where('xml_generated_at', '>=', start_date).stream()
        for doc in query:
            data = doc.to_dict()
            result = {'ecli': doc.id}
            for field in report_fields:
                result[field] = data.get(field, 'Not Available')
            error_docs = db.collection(f'{firestore_collection}_reports').where('ecli', '==', doc.id).stream()
            errors = []
            for error_doc in error_docs:
                error_data = error_doc.to_dict()
                errors.append(error_data.get('status', 'Unknown error'))
            result['errors'] = '; '.join(errors) if errors else 'None'
            results.append(result)

        # Generate reports
        success_rows = [r for r in results if r.get('xml_status') == 'Generated']
        failed_rows = [r for r in results if r.get('xml_status') != 'Generated' or r['errors'] != 'None']
        all_rows = results

        for df, report_type in [
            (pd.DataFrame(success_rows), 'success'),
            (pd.DataFrame(failed_rows), 'failed'),
            (pd.DataFrame(all_rows), 'all')
        ]:
            if not df.empty:
                try:
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    filename = f"{gcs_report_path}/report_{report_type}_{timestamp}.csv"
                    blob = bucket.blob(filename)
                    blob.upload_from_string(df.to_csv(index=False), content_type="text/csv")
                    logger.info(f"Saved {report_type} report to gs://{bucket_name}/{filename}")
                except Exception as e:
                    logger.error(f"Failed to save {report_type} report: {str(e)}")
                    raise

        # Log summary
        logger.info(f"Generated reports: {len(success_rows)} success, {len(failed_rows)} failed, {len(all_rows)} total")

        return {
            'status': 'success',
            'success_count': len(success_rows),
            'failed_count': len(failed_rows),
            'total_count': len(all_rows)
        }

    except Exception as e:
        (logger or fallback_logger).error(f"Unexpected error in generate_reports: {str(e)}", exc_info=True)
        raise