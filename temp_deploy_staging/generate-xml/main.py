import json
import gzip
import xml.etree.ElementTree as ET
import base64
from google.cloud import storage, firestore, pubsub_v1
from src.common.config import load_customer_config, load_project_config
from src.common.utils import generate_url_hash, logger
from datetime import datetime
from xml.dom import minidom
import re

def sanitize_filename(filename):
    """Sanitize filename for GCS by replacing invalid characters."""
    if not filename:
        return "untitled"
    filename = re.sub(r'[^\w\-_\.]', '_', filename)  # Replace invalid chars with underscore
    filename = filename.replace(' ', '_')[:250]  # Truncate to 250 chars
    return filename

def generate_xml(event, context):
    """Cloud Function to generate XML files with customer-specified formatting."""
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
        xml_structure = project_config.get('xml_structure')

        # Validate configuration
        required_fields = {
            'gcs_bucket': bucket_name,
            'firestore_collection': firestore_collection,
            'firestore_database_id': firestore_db_id,
            'xml_structure': xml_structure
        }
        missing_fields = [k for k, v in required_fields.items() if v is None]
        if missing_fields:
            logger.error(f"Missing required configuration fields: {missing_fields}")
            raise ValueError(f"Missing required configuration fields: {missing_fields}")

        filename_field = xml_structure.get('filename_field', 'Bestandsnaam')
        xml_path_template = xml_structure.get('gcs_xml_path', 'delivered_xml/<project_id>/<date>').replace('<project_id>', project_id).replace('<date>', date)

        # Initialize clients
        storage_client = storage.Client(project=customer_config['gcp_project_id'])
        bucket = storage_client.bucket(bucket_name)
        db = firestore.Client(project=customer_config['gcp_project_id'], database=firestore_db_id)
        logger.info(f"Initialized clients for bucket '{bucket_name}', Firestore collection '{firestore_collection}', database '{firestore_db_id}'")

        # List fixed HTML files in GCS
        blobs = bucket.list_blobs(prefix=f"fixed/{project_id}/{date}/")
        batch_size = 10
        results = []

        for blob in blobs:
            if not blob.name.endswith('.html.gz'):
                continue

            filename = blob.name.split('/')[-1].replace('.html.gz', '')
            ecli = filename

            doc_ref = db.collection(firestore_collection).document(ecli)
            doc = doc_ref.get()
            if doc.exists and doc.to_dict().get('xml_path'):
                logger.info(f"Skipping {blob.name}: XML already generated at {doc.to_dict()['xml_path']}")
                results.append({
                    'filename': blob.name,
                    'ecli': ecli,
                    'xml_path': doc.to_dict().get('xml_path'),
                    'status': 'Skipped (already generated)'
                })
                continue

            try:
                html_content = gzip.decompress(blob.download_as_bytes()).decode('utf-8')
            except Exception as e:
                logger.error(f"Failed to decompress {blob.name}: {str(e)}")
                results.append({
                    'filename': blob.name,
                    'ecli': ecli,
                    'status': f'Error: Failed to decompress ({str(e)})'
                })
                continue

            if not doc.exists:
                logger.error(f"No metadata for ECLI {ecli}")
                results.append({
                    'filename': blob.name,
                    'ecli': ecli,
                    'status': 'Error: No metadata'
                })
                continue

            doc_data = doc.to_dict()
            try:
                root = ET.Element(xml_structure['root_tag'])
                for field in xml_structure['fields']:
                    tag = field['tag']
                    source = field['source']
                    if source == 'fixed_html_content':
                        value = html_content
                    else:
                        value = doc_data.get(source, 'Not Available')
                    if field.get('cdata', False):
                        value = f"<![CDATA[{value}]]>"
                    ET.SubElement(root, tag).text = value

                rough_string = ET.tostring(root, encoding='unicode')
                parsed = minidom.parseString(rough_string)
                pretty_xml = parsed.toprettyxml(indent=xml_structure.get('indent', '  '), encoding='UTF-8').decode('UTF-8')

                if xml_structure.get('declaration', True) and not pretty_xml.startswith('<?xml'):
                    pretty_xml = '<?xml version="1.0" encoding="UTF-8"?>\n' + pretty_xml
            except Exception as e:
                logger.error(f"Failed to generate XML for {blob.name}: {str(e)}")
                results.append({
                    'filename': blob.name,
                    'ecli': ecli,
                    'status': f'Error: Failed to generate XML ({str(e)})'
                })
                continue

            filename = sanitize_filename(doc_data.get(filename_field, ecli))
            if not filename.lower().endswith('.xml'):
                filename += '.xml'
            xml_path = f"{xml_path_template}/{filename}"
            xml_blob = bucket.blob(xml_path)
            try:
                xml_blob.upload_from_string(pretty_xml, content_type='application/xml')
                logger.info(f"Saved XML to gs://{bucket_name}/{xml_path}")
            except Exception as e:
                logger.error(f"Failed to upload XML to {xml_path}: {str(e)}")
                results.append({
                    'filename': blob.name,
                    'ecli': ecli,
                    'status': f'Error: Failed to upload XML ({str(e)})'
                })
                continue

            try:
                doc_ref.update({
                    'xml_path': f"gs://{bucket_name}/{xml_path}",
                    'xml_status': 'Generated',
                    'xml_generated_at': firestore.SERVER_TIMESTAMP
                })
                results.append({
                    'filename': blob.name,
                    'ecli': ecli,
                    'xml_path': xml_path,
                    'status': 'Success'
                })
            except Exception as e:
                logger.error(f"Failed to update Firestore for {ecli}: {str(e)}")
                results.append({
                    'filename': blob.name,
                    'ecli': ecli,
                    'xml_path': xml_path,
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
        topic_path = publisher.topic_path(customer_config['gcp_project_id'], 'generate-reports')
        next_message = {
            'customer': customer_id,
            'project': project_id,
            'date': date
        }
        logger.info(f"Attempting to publish to 'generate-reports' with payload: {next_message}")
        future = publisher.publish(topic_path, json.dumps(next_message).encode('utf-8'))
        try:
            msg_id = future.result(timeout=30)
            logger.info(f"Successfully published message {msg_id} to 'generate-reports'.")
        except TimeoutError:
            logger.error("Publishing to 'generate-reports' timed out.", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Failed to publish to 'generate-reports': {str(e)}", exc_info=True)
            raise

        return {
            'status': 'success',
            'processed': len(results),
            'success': success_count
        }

    except Exception as e:
        logger.error(f"Unexpected error in generate_xml: {str(e)}", exc_info=True)
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
    report_path = f"reports/{project_id}/{date}/xml_report_{timestamp}.csv"
    blob = bucket.blob(report_path)
    blob.upload_from_string(df.to_csv(index=False), content_type='text/csv')
    logger.info(f"Saved report to gs://{bucket.name}/{report_path}")