import json
import gzip
import xml.etree.ElementTree as ET
from google.cloud import storage, firestore, pubsub_v1
from src.common.config import load_customer_config, load_project_config
from src.common.utils import generate_url_hash, logger
from datetime import datetime
from xml.dom import minidom

def generate_xml(event, context):
    """Cloud Function to generate XML files with customer-specified formatting."""
    data = json.loads(event['data'].decode('utf-8'))
    customer_id = data['customer']
    project_id = data['project']
    date = data.get('date', datetime.now().strftime('%Y%m%d'))

    # Load configurations
    customer_config = load_customer_config(customer_id)
    project_config = load_project_config(project_id)
    bucket_name = project_config['gcs_bucket']
    firestore_collection = project_config['firestore_collection']
    xml_structure = project_config['xml_structure']
    filename_field = xml_structure.get('filename_field', 'Wets-ID')

    # Initialize clients
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    db = firestore.Client(project=customer_config['gcp_project_id'], database=customer_config['firestore_database'])

    # List fixed HTML files in GCS
    blobs = bucket.list_blobs(prefix=f"fixed/{project_id}/{date}/")
    batch_size = 10
    results = []

    for blob in blobs:
        if not blob.name.endswith('.html.gz'):
            continue

        # Check if XML already exists in Firestore
        url_hash = generate_url_hash(blob.name)
        doc_ref = db.collection(firestore_collection).document(url_hash)
        doc = doc_ref.get().to_dict()
        if doc and doc.get('xml_path') and doc.get('process_status') == 'Processed':
            logger.info(f"Skipping {blob.name}: XML already generated")
            continue

        # Download fixed HTML
        try:
            html_content = gzip.decompress(blob.download_as_bytes()).decode('utf-8')
        except Exception as e:
            logger.error(f"Failed to decompress {blob.name}: {str(e)}")
            results.append({'filename': blob.name, 'status': f'Error: {str(e)}'})
            continue

        # Get metadata from Firestore
        if not doc:
            logger.error(f"No metadata for {blob.name}")
            results.append({'filename': blob.name, 'status': 'No metadata'})
            continue

        # Generate XML
        root = ET.Element(xml_structure['root_tag'])
        for field in xml_structure['fields']:
            tag = field['tag']
            source = field['source']
            value = doc.get(source, 'Not Available')
            if field.get('cdata', False):
                value = f"<![CDATA[{value}]]>"
            ET.SubElement(root, tag).text = value

        # Pretty-print XML with indentation
        rough_string = ET.tostring(root, encoding='unicode')
        parsed = minidom.parseString(rough_string)
        pretty_xml = parsed.toprettyxml(indent=xml_structure.get('indent', '  '), encoding='UTF-8').decode('UTF-8')
        
        # Add XML declaration if specified
        if xml_structure.get('declaration', True):
            if not pretty_xml.startswith('<?xml'):
                pretty_xml = '<?xml version="1.0" encoding="UTF-8"?>\n' + pretty_xml

        # Save XML to GCS
        filename = doc.get(filename_field, doc.get('ECLI', url_hash))
        if not filename.lower().endswith('.xml'):
            filename += '.xml'
        xml_path = f"delivered_xml/{project_id}/{date}/{filename}"
        xml_blob = bucket.blob(xml_path)
        xml_blob.upload_from_string(pretty_xml, content_type='application/xml')
        logger.info(f"Saved XML to gs://{bucket_name}/{xml_path}")

        # Update Firestore
        doc_ref.update({
            'xml_path': f"gs://{bucket_name}/{xml_path}",
            'xml_generated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'file_status': 'Present' if html_content else 'Not Found',
            'process_status': 'Processed'
        })

        results.append({
            'filename': blob.name,
            'xml_path': xml_path,
            'status': 'Success'
        })

        # Process in batches
        if len(results) >= batch_size:
            save_batch_results(results, bucket, project_id, date, firestore_collection)
            results = []

    # Save remaining results
    if results:
        save_batch_results(results, bucket, project_id, date, firestore_collection)

    # Trigger reporting
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(customer_config['gcp_project_id'], 'generate-reports')
    publisher.publish(topic_path, json.dumps({
        'customer': customer_id,
        'project': project_id,
        'date': date
    }).encode('utf-8'))

def save_batch_results(results, bucket, project_id, date, firestore_collection):
    """Save processing results to Firestore and GCS."""
    import pandas as pd
    from datetime import datetime

    # Save to Firestore
    db = firestore.Client()
    for result in results:
        doc_ref = db.collection(f"{firestore_collection}/reports").document(generate_url_hash(result['filename']))
        doc_ref.set(result)

    # Save CSV report to GCS
    df = pd.DataFrame(results)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_path = f"reports/{project_id}/{date}/xml_report_{timestamp}.csv"
    blob = bucket.blob(report_path)
    blob.upload_from_string(df.to_csv(index=False), content_type='text/csv')
    logger.info(f"Saved report to gs://{bucket.name}/{report_path}")