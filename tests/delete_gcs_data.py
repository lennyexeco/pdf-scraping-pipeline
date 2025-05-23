from google.cloud import storage
import logging

# Configuration from your JSON
GCS_BUCKET = "harvey-germany"
PROJECT_ID = "germany_federal_law"
PREFIXES = [
    f"pending/{PROJECT_ID}/",
    f"fixed/{PROJECT_ID}/",
    f"delivered_xml/{PROJECT_ID}/",
    f"reports/{PROJECT_ID}/",
    f"search_results/{PROJECT_ID}/",
    f"search_results_fixed/{PROJECT_ID}/"
]
GCP_PROJECT_ID = "execo-harvey"  # Replace with your actual GCP project ID

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def delete_objects(bucket, prefix):
    """Delete all objects in the given GCS prefix."""
    blobs = bucket.list_blobs(prefix=prefix)
    deleted = 0

    for blob in blobs:
        try:
            blob.delete()
            deleted += 1
            if deleted % 100 == 0:
                logger.info(f"Deleted {deleted} objects from {prefix}")
        except Exception as e:
            logger.error(f"Failed to delete {blob.name}: {str(e)}")

    logger.info(f"Finished deleting {deleted} objects from {prefix}")

def main():
    # Initialize GCS client
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET)

    # Delete objects in each prefix
    for prefix in PREFIXES:
        logger.info(f"Deleting objects in prefix: {prefix}")
        delete_objects(bucket, prefix)

    logger.info("GCS data deletion completed.")

if __name__ == "__main__":
    main()