import gzip
import hashlib
import logging
from google.cloud import storage
from google.cloud import logging as gcp_logging  # Added missing import

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def compress_and_upload(content, bucket_name, destination_blob_name, storage_client=None, content_type="text/html"):
    """Compress and upload content to GCS.

    Args:
        content (str): The content to compress and upload.
        bucket_name (str): The name of the GCS bucket.
        destination_blob_name (str): The destination path in the bucket.
        storage_client (google.cloud.storage.Client, optional): An initialized storage client. If None, a new client is created.
        content_type (str, optional): The MIME type of the content. Defaults to "text/html".

    Returns:
        str: The destination blob name.
    """
    if storage_client is None:
        storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    compressed = gzip.compress(content.encode('utf-8'))
    blob.upload_from_string(compressed, content_type=content_type)
    logger.info(f"Uploaded to gs://{bucket_name}/{destination_blob_name}")
    return destination_blob_name

def generate_url_hash(url):
    """Generate MD5 hash for a URL."""
    return hashlib.md5(url.encode()).hexdigest()

def setup_logging(customer_id, project_id):
    """Set up a Cloud Logging logger with customer and project context."""
    client = gcp_logging.Client()
    logger = client.logger(f"{customer_id}_{project_id}")
    
    # Configure Python logging to use Cloud Logging
    handler = gcp_logging.handlers.CloudLoggingHandler(client)
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(logging.INFO)
    
    # Return a standard Python logger with context
    logger = logging.getLogger(f"{customer_id}_{project_id}")
    logger.info(f"Initialized logger for customer {customer_id}, project {project_id}")
    return logger