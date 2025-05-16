import functions_framework
from google.cloud import storage, pubsub_v1
import json
import paramiko
from src.common.config import load_customer_config, load_project_config
from src.common.utils import generate_url_hash, logger
from datetime import datetime

@functions_framework.cloud_event
def sftp_upload(cloud_event):
    """Cloud Function to upload XML and HTML files to client's SFTP server."""
    data = json.loads(cloud_event.data["message"]["data"].decode("utf-8"))
    customer_id = data["customer"]
    project_id = data["project"]
    date = data.get("date", datetime.now().strftime("%Y%m%d"))

    customer_config = load_customer_config(customer_id)
    project_config = load_project_config(project_id)
    logger.info(f"Uploading files for {project_id} to SFTP")

    # SFTP configuration (replace with actual credentials, possibly from Secret Manager)
    sftp_host = "client.sftp.example.com"
    sftp_port = 22
    sftp_username = get_secret("sftp-username", customer_config["gcp_project_id"])
    sftp_password = get_secret("sftp-password", customer_config["gcp_project_id"])
    sftp_path = "/uploads/"

    # Initialize GCS client
    storage_client = storage.Client(project=customer_config["gcp_project_id"])
    bucket = storage_client.bucket(project_config["gcs_bucket"])

    # List XML files to upload
    blobs = bucket.list_blobs(prefix=f"delivered_xml/{project_id}/{date}/")
    transport = paramiko.Transport((sftp_host, sftp_port))
    transport.connect(username=sftp_username, password=sftp_password)
    sftp = paramiko.SFTPClient.from_transport(transport)

    for blob in blobs:
        if blob.name.endswith(".xml"):
            local_path = f"/tmp/{blob.name.split('/')[-1]}"
            blob.download_to_filename(local_path)
            sftp_path_file = f"{sftp_path}{blob.name.split('/')[-1]}"
            sftp.put(local_path, sftp_path_file)
            logger.info(f"Uploaded {blob.name} to SFTP: {sftp_path_file}")

    sftp.close()
    transport.close()

    return {"status": "success"}