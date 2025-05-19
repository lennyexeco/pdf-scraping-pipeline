import functions_framework
from google.cloud import storage, firestore
import json
import paramiko
import os
from src.common.config import load_customer_config, load_project_config
from src.common.utils import generate_url_hash, logger, get_secret
from datetime import datetime

@functions_framework.cloud_event
def sftp_upload(cloud_event):
    """Cloud Function to upload XML and optional HTML files to client's SFTP server."""
    try:
        # Decode Pub/Sub message
        data = json.loads(cloud_event.data["message"]["data"].decode("utf-8"))
        customer_id = data["customer"]
        project_id = data["project"]
        date = data.get("date", datetime.now().strftime("%Y%m%d"))

        # Load configurations
        customer_config = load_customer_config(customer_id)
        project_config = load_project_config(project_id)
        logger.info(f"Uploading files for {project_id} to SFTP")

        # SFTP configuration
        sftp_config = project_config.get('sftp_config', {})
        sftp_host = sftp_config.get('host', 'client.sftp.example.com')
        sftp_port = sftp_config.get('port', 22)
        sftp_username = get_secret(sftp_config.get('username_secret', 'sftp-username'), customer_config["gcp_project_id"])
        sftp_password = get_secret(sftp_config.get('password_secret', 'sftp-password'), customer_config["gcp_project_id"])
        sftp_path = sftp_config.get('remote_path', '/Uploads/')
        upload_types = sftp_config.get('upload_types', ['xml'])
        batch_size = sftp_config.get('batch_size', 10)

        # Initialize clients
        storage_client = storage.Client(project=customer_config["gcp_project_id"])
        bucket = storage_client.bucket(project_config["gcs_bucket"])
        db = firestore.Client(project=customer_config["gcp_project_id"], database=project_config.get('firestore_database_id'))

        # List files to upload
        results = []
        prefixes = []
        if 'xml' in upload_types:
            prefixes.append(project_config.get('gcs_xml_path', f'delivered_xml/{project_id}/{date}'))
        if 'html' in upload_types:
            prefixes.append(project_config.get('image_url_fix', {}).get('gcs_fixed_path', f'fixed/{project_id}/{date}'))

        # Initialize SFTP connection
        transport = paramiko.Transport((sftp_host, sftp_port))
        try:
            transport.connect(username=sftp_username, password=sftp_password)
            sftp = paramiko.SFTPClient.from_transport(transport)
        except Exception as e:
            logger.error(f"Failed to connect to SFTP server {sftp_host}:{sftp_port}: {str(e)}")
            raise

        for prefix in prefixes:
            blobs = bucket.list_blobs(prefix=prefix)
            batch_results = []

            for blob in blobs:
                if not (blob.name.endswith('.xml') or blob.name.endswith('.html.gz')):
                    continue

                # Extract ECLI from filename
                filename = blob.name.split('/')[-1].replace('.xml', '').replace('.html.gz', '')
                ecli = filename  # Assume filename is ECLI

                # Check Firestore for existing upload
                doc_ref = db.collection(project_config['firestore_collection']).document(ecli)
                doc = doc_ref.get().to_dict()
                if doc and doc.get('sftp_path'):
                    logger.info(f"Skipping {blob.name}: Already uploaded to {doc['sftp_path']}")
                    batch_results.append({
                        'filename': blob.name,
                        'ecli': ecli,
                        'sftp_path': doc['sftp_path'],
                        'status': 'Skipped (already uploaded)'
                    })
                    continue

                # Download file to temporary path
                local_path = f"/tmp/{filename}{'.xml' if blob.name.endswith('.xml') else '.html.gz'}"
                try:
                    blob.download_to_filename(local_path)
                except Exception as e:
                    logger.error(f"Failed to download {blob.name}: {str(e)}")
                    batch_results.append({
                        'filename': blob.name,
                        'ecli': ecli,
                        'status': f'Error: Failed to download ({str(e)})'
                    })
                    continue

                # Upload to SFTP
                sftp_path_file = f"{sftp_path}{filename}{'.xml' if blob.name.endswith('.xml') else '.html.gz'}"
                try:
                    sftp.put(local_path, sftp_path_file)
                    logger.info(f"Uploaded {blob.name} to SFTP: {sftp_path_file}")
                except Exception as e:
                    logger.error(f"Failed to upload {blob.name} to SFTP: {str(e)}")
                    batch_results.append({
                        'filename': blob.name,
                        'ecli': ecli,
                        'status': f'Error: Failed to upload ({str(e)})'
                    })
                    os.remove(local_path)  # Clean up
                    continue

                # Update Firestore
                try:
                    doc_ref.update({
                        'sftp_path': sftp_path_file,
                        'sftp_uploaded_at': firestore.SERVER_TIMESTAMP
                    })
                    batch_results.append({
                        'filename': blob.name,
                        'ecli': ecli,
                        'sftp_path': sftp_path_file,
                        'status': 'Success'
                    })
                except Exception as e:
                    logger.error(f"Failed to update Firestore.ConcurrentModificationException: {e}")
                    batch_results.append({
                        'filename': blob.name,
                        'ecli': ecli,
                        'sftp_path': sftp_path_file,
                        'status': f'Error: Failed to update Firestore ({str(e)})'
                    })

                # Clean up temporary file
                os.remove(local_path)

                # Process in batches
                if len(batch_results) >= batch_size:
                    save_batch_results(batch_results, bucket, project_id, date, project_config['firestore_collection'])
                    results.extend(batch_results)
                    batch_results = []

            # Save remaining batch results
            if batch_results:
                save_batch_results(batch_results, bucket, project_id, date, project_config['firestore_collection'])
                results.extend(batch_results)

        # Close SFTP connection
        sftp.close()
        transport.close()

        # Log summary
        success_count = sum(1 for r in results if r['status'] == 'Success')
        logger.info(f"Processed {len(results)} files: {success_count} successful, {len(results) - success_count} failed")

        return {
            'status': 'success',
            'processed': len(results),
            'success': success_count
        }

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        raise  # Trigger retry via Cloud Functions

def save_batch_results(results, bucket, project_id, date, firestore_collection):
    """Save upload results to Firestore and GCS."""
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
    report_path = f"reports/{project_id}/{date}/sftp_upload_report_{timestamp}.csv"
    blob = bucket.blob(report_path)
    blob.upload_from_string(df.to_csv(index=False), content_type='text/csv')
    logger.info(f"Saved report to gs://{bucket.name}/{report_path}")