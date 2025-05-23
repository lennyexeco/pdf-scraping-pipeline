import json
import base64
import logging
import re
import os
import time
from google.cloud import firestore, pubsub_v1, storage
from src.common.utils import generate_url_hash, setup_logging
from src.common.config import load_customer_config, load_project_config
from datetime import datetime
import functions_framework
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom

logger = logging.getLogger(__name__)
RETRY_TOPIC_NAME = "retry-pipeline"
NEXT_STEP_TOPIC_NAME = "generate-reports"
MAX_GCS_RETRIES = 3
RETRY_BACKOFF = 5  # seconds

def sanitize_filename(filename_base, identifier):
    """Sanitize a base string for a filename, prefixed by identifier, ending with .xml."""
    if not filename_base:
        filename_base = "untitled"
    sanitized_title = re.sub(r'[^\w\-_\.]', '_', str(filename_base)).replace(' ', '_')[:150]
    return f"{identifier}_{sanitized_title}.xml"

def generate_identifier(doc_data, field_mappings):
    """Generate a unique identifier using mapped fields from Firestore document."""
    url_fields = field_mappings.get("Law-ID", {}).get("source", "htmlUrl || url || mainUrl").split(" || ")
    for field in url_fields:
        field = field.strip()
        if doc_data.get(field):
            return generate_url_hash(doc_data[field])
    return generate_url_hash(f"no-id-{datetime.now().isoformat()}")

def get_mapped_field(doc_data, field, field_mappings, logger_instance=None):
    """Get the value for a field based on its mapping in field_mappings."""
    mapping = field_mappings.get(field, {})
    source = mapping.get("source", field)
    field_type = mapping.get("type", "direct")

    if field_type == "computed":
        if "generate_url_hash" in source:
            url_fields = source.replace("generate_url_hash(", "").replace(")", "").split(" || ")
            for url_field in url_fields:
                url_field = url_field.strip()
                if doc_data.get(url_field):
                    return generate_url_hash(doc_data[url_field])
            return generate_url_hash(f"no-id-{datetime.now().isoformat()}")
        return "Not Available"
    else:
        source_fields = source.split(" || ")
        for source_field in source_fields:
            source_field = source_field.strip()
            if doc_data.get(source_field):
                value = doc_data[source_field]
                if isinstance(value, datetime):
                    return value.isoformat()
                return str(value)
        logger_instance.warning(f"No valid source field found for '{field}' in document: {list(doc_data.keys())}")
        return "Not Available"

def custom_pretty_xml(element, xml_structure_config):
    """Generate pretty-printed XML with proper CDATA handling."""
    rough_string = tostring(element, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    pretty_xml = reparsed.toprettyxml(indent=xml_structure_config.get('indent', '  '), encoding='utf-8').decode('utf-8')

    # Manually handle CDATA sections
    for field_config in xml_structure_config.get('fields', []):
        if field_config.get('cdata', False):
            tag = field_config['tag']
            # Escape content and wrap in CDATA
            pretty_xml = re.sub(
                rf'<{tag}>(.*?)</{tag}>',
                lambda m: f'<{tag}><![CDATA[{m.group(1)}]]></{tag}>',
                pretty_xml,
                flags=re.DOTALL
            )

    if xml_structure_config.get('declaration', True):
        if not pretty_xml.strip().startswith('<?xml'):
            pretty_xml = '<?xml version="1.0" encoding="UTF-8"?>\n' + pretty_xml
        else:
            pretty_xml = re.sub(r'<\?xml.*?\?>', '<?xml version="1.0" encoding="UTF-8"?>', pretty_xml, count=1, flags=re.IGNORECASE)
    else:
        pretty_xml = re.sub(r'<\?xml.*?\?>\n?', '', pretty_xml, count=1, flags=re.IGNORECASE).lstrip()

    return pretty_xml

@functions_framework.cloud_event
def generate_xml(cloud_event):
    active_logger = logger
    try:
        pubsub_data_encoded = cloud_event.data.get("message", {}).get("data")
        if not pubsub_data_encoded:
            if isinstance(cloud_event.data, dict) and "customer" in cloud_event.data:
                data = cloud_event.data
                active_logger.info("Interpreting cloud_event.data directly as JSON payload.")
            else:
                raise ValueError("No data in Pub/Sub message or unexpected format.")

        if pubsub_data_encoded:
            data = json.loads(base64.b64decode(pubsub_data_encoded).decode('utf-8'))
            active_logger.info(f"Decoded Pub/Sub message: {data}")

        customer_id = data.get('customer')
        project_id = data.get('project')
        dataset_id = data.get('dataset_id')
        dataset_type = data.get('dataset_type', 'items')
        date = data.get('date', datetime.now().strftime('%Y%m%d'))

        if not all([customer_id, project_id, dataset_id, dataset_type]):
            raise ValueError("Missing required fields (customer, project, dataset_id, dataset_type)")

        active_logger = setup_logging(customer_id, project_id)
        active_logger.info(f"Starting generate_xml for customer: {customer_id}, project: {project_id}, dataset_id: {dataset_id}, type: {dataset_type}, date: {date}")

        customer_config = load_customer_config(customer_id)
        project_config = load_project_config(project_id)

        gcp_project_id = customer_config.get("gcp_project_id", os.environ.get("GCP_PROJECT"))
        if not gcp_project_id:
            raise ValueError("GCP Project ID not configured")

        bucket_name = project_config.get('gcs_bucket')
        firestore_collection = project_config.get('firestore_collection')
        search_results_collection = project_config.get('search_results_collection', f'{firestore_collection}_search_results')
        firestore_db_id = project_config.get('firestore_database_id', '(default)')
        xml_structure = project_config.get('xml_structure')
        field_mappings = project_config.get('field_mappings', {})
        error_collection = f"{firestore_collection}_errors"

        if not all([bucket_name, firestore_collection, xml_structure]):
            raise ValueError("Missing required configuration fields (gcs_bucket, firestore_collection, xml_structure)")

        if dataset_type == 'search_results':
            active_logger.info(f"Skipping XML generation for dataset_type: {dataset_type}")
            publisher = pubsub_v1.PublisherClient()
            topic_path = publisher.topic_path(gcp_project_id, NEXT_STEP_TOPIC_NAME)
            next_message = {
                'customer': customer_id,
                'project': project_id,
                'dataset_id': dataset_id,
                'dataset_type': dataset_type,
                'date': date
            }
            active_logger.info(f"Publishing to '{NEXT_STEP_TOPIC_NAME}': {next_message}")
            future = publisher.publish(topic_path, json.dumps(next_message).encode('utf-8'))
            try:
                msg_id = future.result(timeout=30)
                active_logger.info(f"Published message {msg_id} to '{NEXT_STEP_TOPIC_NAME}'.")
            except TimeoutError:
                active_logger.error(f"Publishing to '{NEXT_STEP_TOPIC_NAME}' timed out.")
                raise
            return {'status': 'success (skipped search_results)', 'processed': 0, 'successful': 0}

        filename_field = xml_structure.get('filename_field', 'Title')
        safe_project_id = "".join(c if c.isalnum() else '_' for c in project_id)
        safe_dataset_id_part = "".join(c if c.isalnum() else '_' for c in dataset_id[:20])
        fixed_html_prefix = project_config.get('fixed_html_gcs_prefix', f"fixed/{safe_project_id}/{date}/{safe_dataset_id_part}")
        pending_html_prefix = project_config.get('pending_html_gcs_prefix', f"pending/{safe_project_id}/{date}/{safe_dataset_id_part}")
        xml_output_prefix = xml_structure.get('gcs_xml_path', f"delivered_xml/{safe_project_id}/{date}/{safe_dataset_id_part}")

        storage_client = storage.Client(project=gcp_project_id)
        bucket = storage_client.bucket(bucket_name)
        db_options = {"project": gcp_project_id}
        if firestore_db_id != "(default)":
            db_options["database"] = firestore_db_id
        db = firestore.Client(**db_options)
        publisher = pubsub_v1.PublisherClient()

        target_collection = search_results_collection if dataset_type == "search_results" else firestore_collection
        docs = db.collection(target_collection).where("html_path", ">=", f"gs://{bucket_name}/{pending_html_prefix}/").where("html_path", "<=", f"gs://{bucket_name}/{pending_html_prefix}/~").stream()
        processed_count = 0
        successful_count = 0
        error_batch = db.batch()
        error_count = 0
        batch_size = 10
        results = []

        for doc in docs:
            doc_data = doc.to_dict()
            identifier = doc.id
            html_path = doc_data.get('fixed_html_path', doc_data.get('html_path', ''))
            expected_blob_suffix = f"{identifier}.{'txt' if dataset_type == 'search_results' else 'html.gz'}"

            if not html_path or not (html_path.startswith(f"gs://{bucket_name}/{fixed_html_prefix}/") or html_path.startswith(f"gs://{bucket_name}/{pending_html_prefix}/")) or not html_path.endswith(expected_blob_suffix):
                active_logger.debug(f"Skipping {identifier}: html_path {html_path} not in expected path or incorrect suffix")
                continue

            if doc_data.get('xml_path'):
                active_logger.info(f"Skipping {identifier}: XML already generated at {doc_data.get('xml_path')}")
                results.append({
                    'identifier': identifier,
                    'status': 'Skipped (already generated)',
                    'xml_path': doc_data.get('xml_path')
                })
                processed_count += 1
                continue

            blob_name = html_path.replace(f"gs://{bucket_name}/", "")
            blob = bucket.blob(blob_name)
            content = ""

            try:
                for attempt in range(MAX_GCS_RETRIES):
                    try:
                        content = gzip.decompress(blob.download_as_bytes()).decode('utf-8') if dataset_type != "search_results" else blob.download_as_string().decode('utf-8')
                        active_logger.debug(f"GCS download successful for {blob_name} on attempt {attempt + 1}")
                        break
                    except Exception as e:
                        active_logger.warning(f"GCS download failed for {blob_name} on attempt {attempt + 1}: {str(e)}")
                        if attempt < MAX_GCS_RETRIES - 1:
                            time.sleep(RETRY_BACKOFF * (2 ** attempt))
                        else:
                            raise Exception(f"Max retries reached for GCS download: {str(e)}")
            except Exception as e:
                active_logger.error(f"Failed to access {blob_name}: {str(e)}")
                error_batch.set(db.collection(error_collection).document(identifier), {
                    "identifier": identifier,
                    "error": f"Download/decompression failed: {str(e)}",
                    "stage": "generate_xml",
                    "retry_count": 0,
                    "original_blob": blob_name,
                    "timestamp": firestore.SERVER_TIMESTAMP
                })
                error_count += 1
                results.append({
                    'identifier': identifier,
                    'status': f"Flagged for retry (download/decompression): {str(e)}",
                    'xml_path': None
                })
                publisher.publish(
                    publisher.topic_path(gcp_project_id, RETRY_TOPIC_NAME),
                    json.dumps({
                        "customer": customer_id,
                        "project": project_id,
                        "dataset_id": dataset_id,
                        "dataset_type": dataset_type,
                        "identifier": identifier,
                        "stage": "generate_xml",
                        "retry_count": 0
                    }).encode('utf-8')
                )
                processed_count += 1
                continue

            try:
                root = Element(xml_structure.get('root_tag', 'Document'))
                for field_config in xml_structure.get('fields', []):
                    tag = field_config['tag']
                    source = field_config['source']
                    value = content if source == "Content" else get_mapped_field(doc_data, source, field_mappings, active_logger)
                    sub_element = SubElement(root, tag)
                    sub_element.text = value

                pretty_xml = custom_pretty_xml(root, xml_structure)
            except Exception as e:
                active_logger.error(f"Failed to generate XML for {identifier}: {str(e)}")
                error_batch.set(db.collection(error_collection).document(identifier), {
                    "identifier": identifier,
                    "error": f"XML generation failed: {str(e)}",
                    "stage": "generate_xml",
                    "retry_count": 0,
                    "original_blob": blob_name,
                    "timestamp": firestore.SERVER_TIMESTAMP
                })
                error_count += 1
                results.append({
                    'identifier': identifier,
                    'status': f"Flagged for retry (XML generation): {str(e)}",
                    'xml_path': None
                })
                publisher.publish(
                    publisher.topic_path(gcp_project_id, RETRY_TOPIC_NAME),
                    json.dumps({
                        "customer": customer_id,
                        "project": project_id,
                        "dataset_id": dataset_id,
                        "dataset_type": dataset_type,
                        "identifier": identifier,
                        "stage": "generate_xml",
                        "retry_count": 0
                    }).encode('utf-8')
                )
                processed_count += 1
                continue

            filename_base = get_mapped_field(doc_data, filename_field, field_mappings, active_logger)
            xml_filename = sanitize_filename(filename_base, identifier)
            xml_gcs_path = f"{xml_output_prefix.rstrip('/')}/{xml_filename}"

            xml_blob = bucket.blob(xml_gcs_path)
            try:
                for attempt in range(MAX_GCS_RETRIES):
                    try:
                        xml_blob.upload_from_string(pretty_xml, content_type='application/xml; charset=utf-8')
                        active_logger.debug(f"GCS upload successful for {xml_gcs_path} on attempt {attempt + 1}")
                        break
                    except Exception as e:
                        active_logger.warning(f"GCS upload failed for {xml_gcs_path} on attempt {attempt + 1}: {str(e)}")
                        if attempt < MAX_GCS_RETRIES - 1:
                            time.sleep(RETRY_BACKOFF * (2 ** attempt))
                        else:
                            raise Exception(f"Max retries reached for GCS upload: {str(e)}")
                active_logger.info(f"Saved XML for {identifier} to gs://{bucket_name}/{xml_gcs_path}")
            except Exception as e:
                active_logger.error(f"Failed to upload XML for {identifier}: {str(e)}")
                error_batch.set(db.collection(error_collection).document(identifier), {
                    "identifier": identifier,
                    "error": f"XML upload failed: {str(e)}",
                    "stage": "generate_xml",
                    "retry_count": 0,
                    "target_gcs_path": xml_gcs_path,
                    "timestamp": firestore.SERVER_TIMESTAMP
                })
                error_count += 1
                results.append({
                    'identifier': identifier,
                    'status': f"Flagged for retry (XML upload): {str(e)}",
                    'xml_path': None
                })
                publisher.publish(
                    publisher.topic_path(gcp_project_id, RETRY_TOPIC_NAME),
                    json.dumps({
                        "customer": customer_id,
                        "project": project_id,
                        "dataset_id": dataset_id,
                        "dataset_type": dataset_type,
                        "identifier": identifier,
                        "stage": "generate_xml",
                        "retry_count": 0
                    }).encode('utf-8')
                )
                processed_count += 1
                continue

            try:
                doc_ref = db.collection(target_collection).document(identifier)
                doc_ref.update({
                    'xml_path': f"gs://{bucket_name}/{xml_gcs_path}",
                    'xml_status': 'Success',
                    'xml_generated_at': firestore.SERVER_TIMESTAMP
                })
                active_logger.info(f"Updated Firestore for {identifier} with XML path.")
                results.append({
                    'identifier': identifier,
                    'status': 'Success',
                    'xml_path': f"gs://{bucket_name}/{xml_gcs_path}"
                })
                successful_count += 1
            except Exception as e:
                active_logger.error(f"Failed to update Firestore for {identifier}: {str(e)}")
                error_batch.set(db.collection(error_collection).document(identifier), {
                    "identifier": identifier,
                    "error": f"Firestore update failed: {str(e)}",
                    "stage": "generate_xml",
                    "retry_count": 0,
                    "xml_gcs_path": xml_gcs_path,
                    "timestamp": firestore.SERVER_TIMESTAMP
                })
                error_count += 1
                results.append({
                    'identifier': identifier,
                    'status': f"Flagged for retry (Firestore update): {str(e)}",
                    'xml_path': f"gs://{bucket_name}/{xml_gcs_path}"
                })
                publisher.publish(
                    publisher.topic_path(gcp_project_id, RETRY_TOPIC_NAME),
                    json.dumps({
                        "customer": customer_id,
                        "project": project_id,
                        "dataset_id": dataset_id,
                        "dataset_type": dataset_type,
                        "identifier": identifier,
                        "stage": "generate_xml",
                        "retry_count": 0
                    }).encode('utf-8')
                )
                successful_count += 1  # XML was generated and uploaded, so count as successful despite Firestore failure

            processed_count += 1

            if len(results) >= batch_size:
                active_logger.info(f"Processed {len(results)} documents in batch.")
                results = []

            if error_count >= 50:
                error_batch.commit()
                active_logger.info(f"Committed {error_count} error documents.")
                error_count = 0
                error_batch = db.batch()

        if error_count > 0:
            error_batch.commit()
            active_logger.info(f"Committed {error_count} error documents.")

        active_logger.info(f"XML Generation summary: Processed {processed_count} documents. Successfully generated {successful_count} XML files.")

        report_id = f"generate_xml_{date}_{safe_project_id}_{safe_dataset_id_part}_{datetime.now().strftime('%H%M%S%f')}"
        report_ref = db.collection(f"{firestore_collection}_reports").document(report_id)
        report_ref.set({
            "report_type": "generate_xml_invocation",
            "customer_id": customer_id,
            "project_id": project_id,
            "dataset_id": dataset_id,
            "dataset_type": dataset_type,
            "processed_count": processed_count,
            "successful_count": successful_count,
            "results_summary": results[:50],
            "processed_at": firestore.SERVER_TIMESTAMP
        })
        active_logger.info(f"Report generated: {report_id}")

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(gcp_project_id, NEXT_STEP_TOPIC_NAME)
        next_message = {
            'customer': customer_id,
            'project': project_id,
            'dataset_id': dataset_id,
            'dataset_type': dataset_type,
            'date': date,
            'status': 'completed',
            'xml_output_gcs_prefix': xml_output_prefix
        }
        active_logger.info(f"Publishing to '{NEXT_STEP_TOPIC_NAME}': {next_message}")
        future = publisher.publish(topic_path, json.dumps(next_message).encode('utf-8'))
        try:
            msg_id = future.result(timeout=30)
            active_logger.info(f"Published message {msg_id} to '{NEXT_STEP_TOPIC_NAME}'.")
        except TimeoutError:
            active_logger.error(f"Publishing to '{NEXT_STEP_TOPIC_NAME}' timed out.")
            raise

        return {'status': 'success', 'processed': processed_count, 'successful': successful_count, 'report_id': report_id}

    except Exception as e:
        active_logger.error(f"Critical error in generate_xml: {str(e)}", exc_info=True)
        raise