import gzip
import hashlib
import logging
from google.cloud import storage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def compress_and_upload(content, bucket_name, destination_blob_name):
    bucket = storage.Client().bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    compressed = gzip.compress(content.encode())
    blob.upload_from_string(compressed, content_type="text/html")
    logger.info(f"Uploaded to gs://{bucket_name}/{destination_blob_name}")
    return destination_blob_name

def generate_url_hash(url):
    return hashlib.md5(url.encode()).hexdigest()