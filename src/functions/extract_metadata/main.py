import base64
import json
import os
import logging
from google.cloud import firestore, pubsub_v1
from google.cloud import secretmanager
from apify_client import ApifyClient
from src.common.utils import generate_url_hash # Assuming this is in your shared common utils
from concurrent.futures import TimeoutError # Explicit import for clarity

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration base path
CONFIG_BASE_PATH = os.path.join(os.path.dirname(__file__), 'src', 'configs')


# --- Constants for processing control ---
MAX_ITEMS_PER_INVOCATION = int(os.environ.get("MAX_ITEMS_PER_INVOCATION", 250))
APIFY_BATCH_SIZE = int(os.environ.get("APIFY_BATCH_SIZE", 50))
SELF_TRIGGER_TOPIC_NAME = "process-data"
NEXT_STEP_TOPIC_NAME = "store-html"


def load_config(customer_name, project_name):
    """Loads the project-specific configuration file."""
    config_file_path = os.path.join(CONFIG_BASE_PATH, 'projects', f"{project_name}.json")
    logger.info(f"Attempting to load configuration from: {config_file_path}")

    if not os.path.exists(config_file_path):
        logger.error(f"Configuration file not found at {config_file_path}")
        alt_config_path = os.path.join(os.path.dirname(__file__),'..', 'src', 'configs', 'projects', f"{project_name}.json")
        if os.path.exists(alt_config_path):
            config_file_path = alt_config_path
            logger.info(f"Found config at alternative path for local testing: {config_file_path}")
        else:
            raise FileNotFoundError(f"Configuration file not found for project {project_name} at {config_file_path} or {alt_config_path}")

    try:
        with open(config_file_path, 'r') as f:
            config = json.load(f)
        logger.info(f"Successfully loaded configuration for project: {project_name}")
        return config
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from {config_file_path}: {e}")
        raise
    except Exception as e:
        logger.error(f"Error loading config file {config_file_path}: {e}")
        raise

def get_secret(secret_id, project_id):
    """Retrieve a secret from Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    try:
        response = client.access_secret_version(name=name)
        secret_value = response.payload.data.decode("UTF-8")
        logger.info(f"Successfully retrieved secret: {secret_id}")
        return secret_value
    except Exception as e:
        logger.error(f"Failed to retrieve secret {secret_id}: {e}")
        raise

def extract_metadata(event, context):
    """
    Cloud Function to extract metadata from Apify dataset, store in Firestore,
    and re-trigger itself if more data exists, otherwise trigger next step.
    """
    logger.info(f"Function triggered by messageId: {context.event_id} published at {context.timestamp}")
    gcp_project_id = os.environ.get("GCP_PROJECT", "execo-harvey")

    try:
        if 'data' in event:
            pubsub_message_str = base64.b64decode(event['data']).decode('utf-8')
            pubsub_message = json.loads(pubsub_message_str)
            logger.info(f"Decoded Pub/Sub message: {pubsub_message}")
        else:
            logger.error("No data found in the event.")
            raise ValueError("No data in Pub/Sub message")

        customer = pubsub_message.get("customer")
        project_config_name = pubsub_message.get("project")
        dataset_id = pubsub_message.get("dataset_id")
        current_offset = int(pubsub_message.get("offset", 0))

        if not all([customer, project_config_name, dataset_id]):
            logger.error("Missing 'customer', 'project', or 'dataset_id' in Pub/Sub message.")
            raise ValueError("Missing required Pub/Sub message fields")

        logger.info(f"Processing dataset_id: {dataset_id} for customer: {customer}, project: {project_config_name}, starting at offset: {current_offset}")

        config = load_config(customer, project_config_name)
        required_fields = config.get("required_fields", [])
        firestore_collection = config.get("firestore_collection")
        firestore_db_id = config.get("firestore_database_id")

        db_options = {"project": gcp_project_id}
        if firestore_db_id:
            db_options["database"] = firestore_db_id
        db = firestore.Client(**db_options)
        logger.info(f"Initialized Firestore client for project '{gcp_project_id}', database '{firestore_db_id or '(default)'}'")

        apify_key_secret_name = config.get("apify_api_key_secret", "apify-api-key")
        apify_key = get_secret(apify_key_secret_name, gcp_project_id)
        apify_client = ApifyClient(apify_key)

        publisher = pubsub_v1.PublisherClient()

        total_items_processed_in_this_invocation = 0
        dataset_exhausted = False
        results_summary = []

        while total_items_processed_in_this_invocation < MAX_ITEMS_PER_INVOCATION:
            logger.info(f"Fetching Apify data. Dataset: {dataset_id}, Offset: {current_offset}, Limit: {APIFY_BATCH_SIZE}")
            try:
                response = apify_client.dataset(dataset_id).list_items(
                    offset=current_offset,
                    limit=APIFY_BATCH_SIZE
                )
                items_in_apify_batch = response.items
            except Exception as e:
                logger.error(f"Error fetching Apify data at offset {current_offset}: {str(e)}")
                results_summary.append({"offset": current_offset, "status": f"Apify fetch error: {str(e)}"})
                dataset_exhausted = True
                break

            if not items_in_apify_batch:
                logger.info("No more items found in Apify dataset.")
                dataset_exhausted = True
                break

            logger.info(f"Fetched {len(items_in_apify_batch)} items from Apify.")
            firestore_batch = db.batch()
            items_in_current_firestore_batch = 0

            for item in items_in_apify_batch:
                ecli = item.get("ecliId", item.get("uniqueId", ""))
                item_url = item.get("url", "N/A")

                if not ecli:
                    if item_url == "N/A" or not item_url:
                        logger.warning(f"No ECLI and no URL for an item. Skipping. Item data: {item.get('title', 'No title')}")
                        results_summary.append({"ecli": "UNKNOWN_NO_ID_NO_URL", "url": item_url, "status": "Skipped - No ECLI or URL"})
                        continue
                    ecli = generate_url_hash(item_url)
                    logger.warning(f"No ECLI found for item (URL: {item_url}), using URL hash: {ecli}")

                metadata = {}
                for field in required_fields:
                    if field == "Wets-ID": metadata[field] = item.get("ecliId", "Not Available")
                    elif field == "Wets-URL": metadata[field] = item_url
                    elif field == "Bestandsnaam": metadata[field] = item.get("title", "untitled").replace(" ", "_")[:250]
                    elif field == "WetInhoud": metadata[field] = item.get("content", "Not Available")
                    else: metadata[field] = item.get(field, "Not Available")

                metadata["scrape_date"] = item.get("extractionTimestamp", firestore.SERVER_TIMESTAMP)
                metadata["processed_at_extract_metadata"] = firestore.SERVER_TIMESTAMP

                doc_ref = db.collection(firestore_collection).document(ecli)
                firestore_batch.set(doc_ref, metadata)
                items_in_current_firestore_batch += 1
                results_summary.append({"ecli": ecli, "url": item_url, "status": "Queued for Firestore"})

            if items_in_current_firestore_batch > 0:
                try:
                    firestore_batch.commit()
                    logger.info(f"Committed batch of {items_in_current_firestore_batch} documents to Firestore.")
                    for res in results_summary[-items_in_current_firestore_batch:]:
                        if res["status"] == "Queued for Firestore":
                            res["status"] = "Success (Firestore)"
                except Exception as e:
                    logger.error(f"Error committing Firestore batch: {str(e)}")
                    for res in results_summary[-items_in_current_firestore_batch:]:
                         if res["status"] == "Queued for Firestore":
                            res["status"] = f"Error (Firestore commit): {str(e)}"

            total_items_processed_in_this_invocation += len(items_in_apify_batch)
            current_offset += len(items_in_apify_batch)

            if len(items_in_apify_batch) < APIFY_BATCH_SIZE:
                logger.info("Fetched fewer items than batch size, assuming end of dataset for this Apify call.")
                dataset_exhausted = True
                break

        logger.info(f"Processed {total_items_processed_in_this_invocation} items in this invocation. Current total offset: {current_offset}.")
        success_count = sum(1 for r in results_summary if "Success" in r["status"])
        logger.info(f"Summary for this invocation: {len(results_summary)} items attempted, {success_count} successful saves.")

        if not dataset_exhausted and total_items_processed_in_this_invocation >= MAX_ITEMS_PER_INVOCATION :
            next_pubsub_message = {
                "customer": customer, "project": project_config_name,
                "dataset_id": dataset_id, "offset": current_offset
            }
            topic_path_self_trigger = publisher.topic_path(gcp_project_id, SELF_TRIGGER_TOPIC_NAME)
            logger.info(f"Attempting to publish re-trigger message to topic '{SELF_TRIGGER_TOPIC_NAME}' for offset {current_offset} with payload: {next_pubsub_message}")
            future = publisher.publish(topic_path_self_trigger, json.dumps(next_pubsub_message).encode('utf-8'))
            try:
                message_id = future.result(timeout=30)
                logger.info(f"Successfully published re-trigger message_id: {message_id} to '{SELF_TRIGGER_TOPIC_NAME}' for offset {current_offset}.")
            except TimeoutError:
                logger.error(f"Publishing re-trigger message to '{SELF_TRIGGER_TOPIC_NAME}' for offset {current_offset} timed out.")
                raise
            except Exception as e:
                logger.error(f"Failed to publish re-trigger message to '{SELF_TRIGGER_TOPIC_NAME}' for offset {current_offset}: {e}", exc_info=True)
                raise
        else:
            logger.info(f"Dataset {dataset_id} processing complete by extract-metadata (or processing halted). Triggering next step: '{NEXT_STEP_TOPIC_NAME}'.")
            next_step_message = {
                "customer": customer, "project": project_config_name, "dataset_id": dataset_id
            }
            topic_path_next_step = publisher.topic_path(gcp_project_id, NEXT_STEP_TOPIC_NAME)
            logger.info(f"Attempting to publish next-step message to topic '{NEXT_STEP_TOPIC_NAME}' with payload: {next_step_message}")
            future_next_step = publisher.publish(topic_path_next_step, json.dumps(next_step_message).encode('utf-8'))
            try:
                message_id_next_step = future_next_step.result(timeout=30)
                logger.info(f"Successfully published next-step message_id: {message_id_next_step} to '{NEXT_STEP_TOPIC_NAME}'.")
            except TimeoutError:
                logger.error(f"Publishing next-step message to '{NEXT_STEP_TOPIC_NAME}' timed out.")
                raise
            except Exception as e:
                logger.error(f"Failed to publish next-step message to '{NEXT_STEP_TOPIC_NAME}': {e}", exc_info=True)
                raise

    except FileNotFoundError as e:
        logger.error(f"Configuration file error: {str(e)}", exc_info=True)
    except ValueError as e:
        logger.error(f"Data error: {str(e)}", exc_info=True)
    except Exception as e:
        logger.error(f"An unexpected error occurred in extract_metadata: {str(e)}", exc_info=True)
        raise

if __name__ == '__main__':
    from datetime import datetime # Added for MockContext
    print("Simulating local function call for extract_metadata...")
    os.environ["GCP_PROJECT"] = "execo-harvey"
    # os.environ["MAX_ITEMS_PER_INVOCATION"] = "10" # For faster local test cycles
    # os.environ["APIFY_BATCH_SIZE"] = "5"      # For faster local test cycles

    dummy_config_dir = os.path.join(os.path.dirname(__file__), 'src', 'configs', 'projects')
    os.makedirs(dummy_config_dir, exist_ok=True)
    dummy_config_file = os.path.join(dummy_config_dir, "netherlands.json")

    if not os.path.exists(dummy_config_file):
        print(f"Creating dummy config at {dummy_config_file}")
        with open(dummy_config_file, "w") as f:
            json.dump({
                "required_fields": ["ecliId", "url", "title", "content", "Wets-ID", "Wets-URL", "Bestandsnaam", "WetInhoud"],
                "firestore_collection": "netherlands_test_local",
                "firestore_database_id": "",
                "apify_api_key_secret": "apify-api-key" # Ensure this exists if testing against live GCP
            }, f, indent=2)

    mock_initial_event_data = {
        "customer": "harvey", "project": "netherlands", "dataset_id": "CebgWL0NgapQr9rRO"
    }
    mock_event = {
        'data': base64.b64encode(json.dumps(mock_initial_event_data).encode('utf-8')).decode('utf-8')
    }

    class MockContext:
        event_id = 'mock_event_id_local_full_code'
        timestamp = datetime.utcnow().isoformat() + "Z"

    # Mock publisher for local testing to avoid actual Pub/Sub calls
    class MockPublisher:
        def topic_path(self, project, topic):
            return f"projects/{project}/topics/{topic}"
        def publish(self, topic_path, data):
            logger.info(f"[Mocked Publish] To: {topic_path}, Data: {data.decode('utf-8')}")
            class MockFuture:
                def result(self, timeout=None):
                    return "mock_message_id_12345"
            return MockFuture()

    original_publisher = pubsub_v1.PublisherClient # Keep a reference
    pubsub_v1.PublisherClient = MockPublisher      # Monkey patch with mock

    try:
        extract_metadata(mock_event, MockContext())
    except Exception as e:
        print(f"Local simulation encountered an error: {e}")
    finally:
        pubsub_v1.PublisherClient = original_publisher # Restore original
        print("Local simulation finished.")