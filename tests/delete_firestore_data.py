from google.cloud import firestore
import logging

# Configuration from your JSON
PROJECT_ID = "germany_federal_law"
FIRESTORE_DB_ID = "harvey-au"
COLLECTIONS = [
    "germany_federal_law",
    "germany_search_results",
    "germany_federal_law_errors",
    "germany_federal_law_reports"
]
GCP_PROJECT_ID = "execo-harvey"  # Replace with your actual GCP project ID

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def delete_collection(db, collection_ref, batch_size=100):
    """Recursively delete all documents in a collection and its subcollections."""
    docs = collection_ref.limit(batch_size).stream()
    deleted = 0

    for doc in docs:
        # Delete subcollections first
        for subcollection in doc.reference.collections():
            delete_collection(db, subcollection, batch_size)
        # Delete the document
        doc.reference.delete()
        deleted += 1
        if deleted % batch_size == 0:
            logger.info(f"Deleted {deleted} documents from {collection_ref.id}")

    if deleted > 0:
        logger.info(f"Finished deleting {deleted} documents from {collection_ref.id}")

def main():
    # Initialize Firestore client
    db = firestore.Client(project=GCP_PROJECT_ID, database=FIRESTORE_DB_ID)

    # Delete each collection and its subcollections
    for collection_name in COLLECTIONS:
        logger.info(f"Deleting collection: {collection_name}")
        collection_ref = db.collection(collection_name)
        delete_collection(db, collection_ref)

        # Explicitly delete known subcollections (for clarity)
        for subcollection in [f"{collection_name}_errors", f"{collection_name}_reports"]:
            logger.info(f"Deleting subcollection: {subcollection}")
            subcollection_ref = db.collection(subcollection)
            delete_collection(db, subcollection_ref)

    logger.info("Firestore data deletion completed.")

if __name__ == "__main__":
    main()