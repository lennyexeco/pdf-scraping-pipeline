from google.cloud import firestore
from google.api_core.retry import Retry
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    db = firestore.Client(project="execo-harvey", database="harvey-au")
    doc_ref = db.collection("netherlands").document("test")
    # Define retry policy for transient errors
    retry_policy = Retry(predicate=lambda exc: isinstance(exc, Exception), initial=1.0, maximum=60.0)
    retry_policy(lambda: doc_ref.set({
        "test_field": "test_value",
        "scrape_date": firestore.SERVER_TIMESTAMP
    }))()
    logger.info("Wrote test document")
except Exception as e:
    logger.error(f"Failed to write to Firestore: {str(e)}", exc_info=True)
    raise