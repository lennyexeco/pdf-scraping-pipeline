import json
import base64
import logging
import os
import time
from google.cloud import firestore, storage, pubsub_v1
# Updated import to include load_dynamic_site_config
from src.common.config import load_customer_config, load_dynamic_site_config
from src.common.helpers import get_mapped_field, sanitize_error_message, serialize_firestore_doc
from src.common.utils import setup_logging
from datetime import datetime, timezone
import functions_framework
import pandas as pd

logger = logging.getLogger(__name__)

# Constants
RETRY_TOPIC_NAME = "retry-pipeline"
MAX_GCS_RETRIES = 3
MAX_FIRESTORE_RETRIES = 3
RETRY_BACKOFF = 5  # seconds

@functions_framework.cloud_event
def generate_reports(cloud_event):
    """
    Generates reports based on processed data in Firestore.
    Uses dynamic site configuration potentially merged with static fallbacks.
    """
    active_logger = logger
    publisher = None
    db = None # Initialize db earlier for dynamic config loading

    try:
        pubsub_message_data_encoded = cloud_event.data.get("message", {}).get("data")
        if not pubsub_message_data_encoded:
            active_logger.error("No 'data' in Pub/Sub message envelope.")
            raise ValueError("No 'data' in Pub/Sub message envelope.")

        data = json.loads(base64.b64decode(pubsub_message_data_encoded).decode('utf-8'))
        
        customer_id = data.get('customer')
        project_id_config_name = data.get('project')
        report_date_str = data.get('report_date', datetime.now(timezone.utc).strftime('%Y%m%d'))
        processing_batch_id = data.get('processing_batch_id')

        if not all([customer_id, project_id_config_name]):
            missing_fields_list = [
                f_name for f_name, f_val in {
                    "customer": customer_id, "project": project_id_config_name
                }.items() if not f_val
            ]
            active_logger.error(f"Missing required PubSub fields: {', '.join(missing_fields_list)}. Data: {str(data)[:500]}")
            raise ValueError(f"Missing required fields: {', '.join(missing_fields_list)}")

        active_logger = setup_logging(customer_id, project_id_config_name)
        active_logger.info(f"Generating report for customer '{customer_id}', project '{project_id_config_name}', date '{report_date_str}', batch ID '{processing_batch_id}'.")

        customer_config = load_customer_config(customer_id)
        gcp_project_id = customer_config.get('gcp_project_id', os.environ.get("GCP_PROJECT"))
        
        if not gcp_project_id:
            active_logger.error("GCP Project ID not found in customer_config or environment.")
            raise ValueError("GCP Project ID not configured.")

        # Initialize Firestore client earlier to pass to load_dynamic_site_config
        # project_config will be loaded by load_dynamic_site_config which also handles merging
        # static and Firestore-based (AI-generated) configs.
        
        # Determine Firestore Database ID from static customer or project config if needed for client init
        # Assuming load_customer_config or a base project config might hint at the database_id
        # For simplicity, we'll assume the default DB or that load_dynamic_site_config
        # can get the db_id from a preliminary static load if necessary.
        # Let's load a minimal static config first if DB ID is there.
        prelim_static_project_config = None
        try:
            # This is your original function to load the base static config.
            # We need it if firestore_database_id is in the static project config.
            from src.common.config import load_project_config as load_static_project_config
            prelim_static_project_config = load_static_project_config(project_id_config_name)
            firestore_db_id_static = prelim_static_project_config.get('firestore_database_id', '(default)')
        except Exception as e_cfg_load:
            active_logger.warning(f"Could not load preliminary static config for DB ID, defaulting. Error: {e_cfg_load}")
            firestore_db_id_static = '(default)'


        db_options = {"project": gcp_project_id}
        if firestore_db_id_static != "(default)":
            db_options["database"] = firestore_db_id_static
        db = firestore.Client(**db_options)
        
        # Now load the potentially merged dynamic and static project configuration
        project_config = load_dynamic_site_config(db, project_id_config_name, active_logger)
        if not project_config: # load_dynamic_site_config should always return a config (either merged or static)
            active_logger.error(f"Failed to load project configuration for {project_id_config_name}.")
            raise ValueError(f"Project configuration for {project_id_config_name} could not be loaded.")


        bucket_name = project_config.get('gcs_bucket')
        firestore_collection_name = project_config.get('firestore_collection')
        report_config = project_config.get('report_config', {})
        field_mappings = project_config.get('field_mappings', {}) # This will now use AI-enhanced mappings if available
        error_collection_name = f"{firestore_collection_name}_errors"
        # The firestore_db_id for queries should be consistent with the one used for the db client.
        # If project_config (dynamic) overrides it, ensure consistency or use the client's DB.
        # For this example, we assume the db client is already correctly initialized for the target database.

        if not all([bucket_name, firestore_collection_name, report_config]):
            missing_configs = [
                name for name, val in {
                    "gcs_bucket": bucket_name, 
                    "firestore_collection": firestore_collection_name,
                    "report_config": report_config # report_config being empty might be acceptable if defaults are fine
                }.items() if not val
            ]
            active_logger.error(f"Missing critical configuration from project_config: {', '.join(missing_configs)}")
            raise ValueError(f"Missing critical configuration from project_config: {', '.join(missing_configs)}")

        storage_client = storage.Client(project=gcp_project_id)
        bucket = storage_client.bucket(bucket_name)
        publisher = pubsub_v1.PublisherClient()

        # Report fields can now be influenced by the AI-generated schema if it includes report configuration
        report_fields_config = report_config.get('item_fields', []) 
        # Define default columns and add dynamically configured ones
        base_columns = ['identifier', 'main_url', 'processing_status', 'xml_path']
        # Ensure no duplicates if item_fields might contain base_columns, though unlikely
        report_columns_set = set(base_columns + report_fields_config)
        # Preserve order: start with base, then add unique fields from config
        report_columns = base_columns + [f for f in report_fields_config if f not in base_columns]
        
        active_logger.info(f"Report columns: {report_columns}")

        query_collection_ref = db.collection(firestore_collection_name)
        
        try:
            start_datetime = datetime.strptime(report_date_str, '%Y%m%d').replace(tzinfo=timezone.utc)
            end_datetime = start_datetime.replace(hour=23, minute=59, second=59, microsecond=999999)
        except ValueError:
            active_logger.error(f"Invalid report_date_str format: {report_date_str}. Expected YYYYMMDD.")
            raise ValueError(f"Invalid report_date_str format: {report_date_str}.")

        query = query_collection_ref.where(filter=firestore.FieldFilter('scrape_timestamp', '>=', start_datetime)) \
                                     .where(filter=firestore.FieldFilter('scrape_timestamp', '<=', end_datetime))
        
        if processing_batch_id:
            query = query.where(filter=firestore.FieldFilter('processing_batch_id', '==', processing_batch_id))
            active_logger.info(f"Filtering report by processing_batch_id: {processing_batch_id}")
        else:
            active_logger.info(f"No processing_batch_id provided, reporting on all items for {report_date_str}.")

        report_data = []
        docs_stream = None
        try:
            for attempt in range(MAX_FIRESTORE_RETRIES):
                try:
                    docs_stream = query.stream()
                    break 
                except Exception as e:
                    if "index" in str(e).lower() or "requires an index" in str(e).lower():
                        active_logger.error(f"Firestore query failed due to a missing index: {str(e)}")
                        return {
                            'status': 'error',
                            'message': f"Missing or misconfigured Firestore index. Collection: '{firestore_collection_name}', Fields involved: 'scrape_timestamp', 'processing_batch_id'. Error: {str(e)}",
                            'processed_items': 0
                        }
                    active_logger.warning(f"Firestore query attempt {attempt + 1}/{MAX_FIRESTORE_RETRIES} failed: {str(e)}")
                    if attempt < MAX_FIRESTORE_RETRIES - 1:
                        time.sleep(RETRY_BACKOFF * (2 ** attempt))
                    else:
                        active_logger.error(f"Max retries reached for Firestore query. Last error: {str(e)}")
                        raise 

            if docs_stream is None:
                 active_logger.error("Failed to obtain document stream from Firestore after retries.")
                 raise Exception("Failed to obtain document stream from Firestore.")

            for doc in docs_stream:
                doc_data = doc.to_dict()
                doc_id = doc.id 

                error_messages_list = []
                try:
                    # Ensure you have an index on 'identifier' in the error_collection
                    error_query = db.collection(error_collection_name).where(filter=firestore.FieldFilter('identifier', '==', doc_id))
                    error_docs_stream = error_query.stream()
                    for error_doc in error_docs_stream:
                        error_messages_list.append(error_doc.to_dict().get('error', 'Unknown error recorded'))
                except Exception as e_err:
                    active_logger.warning(f"Could not fetch errors for {doc_id}: {str(e_err)}")
                
                errors_str = '; '.join(error_messages_list) if error_messages_list else 'None'

                row = {
                    'identifier': doc_id,
                    'errors': errors_str,
                    'main_url': doc_data.get('main_url', 'Not Available'),
                    'processing_status': doc_data.get('processing_status', 'Unknown'),
                    'xml_path': doc_data.get('xml_path', 'Not Generated')
                }

                for field_key in report_fields_config: # Iterate only through fields defined in item_fields
                    # get_mapped_field will use the (potentially AI-enhanced) field_mappings
                    value = get_mapped_field(doc_data, field_key, field_mappings, logger_instance=active_logger)
                    # Fallback if get_mapped_field returns None or "Not Available"
                    if value is None or value == "Not Available":
                        value = doc_data.get(field_key, 'Not Available') 
                    row[field_key] = str(value)
                report_data.append(row)
            active_logger.info(f"Collected {len(report_data)} items for the report.")

        except Exception as e_query:
            active_logger.error(f"Critical error during Firestore data collection for report: {str(e_query)}", exc_info=True)
            retry_payload = {
                "customer": customer_id, "project": project_id_config_name, "report_date": report_date_str,
                "processing_batch_id": processing_batch_id, "stage": "generate_reports_query_failure",
                "error_message": sanitize_error_message(str(e_query)),
                "retry_count": data.get("retry_count", 0) + 1, "original_pubsub_message": data
            }
            if publisher:
                publisher.publish(
                    publisher.topic_path(gcp_project_id, RETRY_TOPIC_NAME),
                    json.dumps(retry_payload, default=serialize_firestore_doc).encode('utf-8')
                )
            return {'status': 'error', 'message': f"Firestore data collection for report failed: {str(e_query)}", 'processed_items': 0}

        safe_project_path_component = "".join(c if c.isalnum() else '_' for c in project_id_config_name)
        report_filename_suffix = f"{processing_batch_id}_{report_date_str}" if processing_batch_id else report_date_str
        
        gcs_report_base_path = report_config.get('gcs_report_path_template', f"reports/{safe_project_path_component}")
        gcs_report_base_path = gcs_report_base_path.strip('/')

        report_file_name = f"pipeline_report_{safe_project_path_component}_{report_filename_suffix}_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}.csv"
        full_gcs_report_path = f"{gcs_report_base_path}/{report_date_str}/{report_file_name}" 
        
        report_blob = bucket.blob(full_gcs_report_path)

        if not report_data:
            active_logger.info(f"No data found for report criteria. Creating empty report with headers: {report_columns}")
            csv_content = pd.DataFrame(columns=report_columns).to_csv(index=False) # Ensures headers are present
        else:
            # Ensure DataFrame uses the exact column order and set defined in report_columns
            df = pd.DataFrame(report_data)
            # Reorder/select columns to match report_columns specification
            df = df.reindex(columns=report_columns, fill_value='Not Available')
            csv_content = df.to_csv(index=False)


        try:
            for attempt in range(MAX_GCS_RETRIES):
                try:
                    report_blob.upload_from_string(csv_content, content_type='text/csv')
                    active_logger.info(f"Report successfully uploaded to: gs://{bucket_name}/{full_gcs_report_path}")
                    break 
                except Exception as e_gcs_upload:
                    active_logger.warning(f"GCS report upload attempt {attempt + 1}/{MAX_GCS_RETRIES} failed: {str(e_gcs_upload)}")
                    if attempt < MAX_GCS_RETRIES - 1:
                        time.sleep(RETRY_BACKOFF * (2 ** attempt))
                    else:
                        active_logger.error(f"Max retries for GCS report upload. Error: {str(e_gcs_upload)}")
                        raise 
        except Exception as e_gcs_final:
            active_logger.error(f"Failed to upload report to GCS: {str(e_gcs_final)}", exc_info=True)
            retry_payload_gcs = {
                "customer": customer_id, "project": project_id_config_name, "report_date": report_date_str,
                "processing_batch_id": processing_batch_id, "stage": "generate_reports_gcs_upload_failure",
                "error_message": sanitize_error_message(str(e_gcs_final)), "retry_count": data.get("retry_count", 0) + 1,
                "target_gcs_path": f"gs://{bucket_name}/{full_gcs_report_path}", "original_pubsub_message": data
            }
            if publisher:
                publisher.publish(
                    publisher.topic_path(gcp_project_id, RETRY_TOPIC_NAME),
                    json.dumps(retry_payload_gcs, default=serialize_firestore_doc).encode('utf-8')
                )
            return {'status': 'error', 'message': f"Report GCS upload failed: {str(e_gcs_final)}", 'processed_items': len(report_data)}
            
        active_logger.info(f"Report generation complete. {len(report_data)} items included.")
        return {'status': 'success', 'processed_items': len(report_data), 'report_gcs_path': f"gs://{bucket_name}/{full_gcs_report_path}"}

    except Exception as e_critical:
        active_logger.error(f"Critical unhandled error in generate_reports: {str(e_critical)}", exc_info=True)
        gcp_project_id_fallback = os.environ.get("GCP_PROJECT")
        current_gcp_project_id_for_error = gcp_project_id if 'gcp_project_id' in locals() and gcp_project_id else gcp_project_id_fallback

        if current_gcp_project_id_for_error and ('customer_id' in locals() and customer_id) and ('project_id_config_name' in locals() and project_id_config_name):
            critical_error_payload = {
                "customer": customer_id, "project": project_id_config_name, "stage": "generate_reports_critical_failure",
                "error_message": sanitize_error_message(str(e_critical)),
                "retry_count": data.get("retry_count", 0) + 1 if 'data' in locals() else 1,
                "original_pubsub_message": data if 'data' in locals() else "Payload not available",
                "report_date_attempted": report_date_str if 'report_date_str' in locals() else "Unknown",
                "processing_batch_id_attempted": processing_batch_id if 'processing_batch_id' in locals() else "Unknown"
            }
            try:
                if publisher is None: publisher = pubsub_v1.PublisherClient()
                publisher.publish(
                    publisher.topic_path(current_gcp_project_id_for_error, RETRY_TOPIC_NAME),
                    json.dumps(critical_error_payload, default=serialize_firestore_doc).encode('utf-8')
                )
            except Exception as e_pub_retry:
                active_logger.error(f"Failed to publish critical error details to {RETRY_TOPIC_NAME}: {str(e_pub_retry)}")
        else:
             active_logger.error("Insufficient context to publish critical error details to retry topic.")
        
        return {'status': 'error', 'message': f"Critical error in generate_reports: {str(e_critical)}", 'processed_items': 0}