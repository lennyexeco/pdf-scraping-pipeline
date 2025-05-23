import gzip
import hashlib
import logging
from google.cloud import storage
from google.cloud import logging as gcp_logging  # Added missing import

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def compress_and_upload(content, bucket_name, destination_blob_name):
    """Compress and upload content to GCS."""
    bucket = storage.Client().bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    compressed = gzip.compress(content.encode())
    blob.upload_from_string(compressed, content_type="text/html")
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