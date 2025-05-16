import functions_framework
from google.cloud import pubsub_v1
import json
from apify_client import ApifyClient
from src.common.config import load_customer_config, load_project_config, get_secret
from src.common.utils import compress_and_upload, generate_url_hash, setup_logging
from datetime import datetime

@functions_framework.cloud_event
def store_html(cloud_event):
    data = json.loads(cloud_event.data["message"]["data"].decode("utf-8"))
    customer_id = data["customer"]
    project_id = data["project"]
    dataset_id = data["dataset_id"]

    customer_config = load_customer_config(customer_id)
    project_config = load_project_config(project_id)
    logger = setup_logging(customer_id, project_id)

    apify_key = get_secret(customer_config["apify_api_key_secret"], customer_config["gcp_project_id"])
    client = ApifyClient(apify_key)

    batch_size = project_config.get("batch_size", 50)
    items = client.dataset(dataset_id).list_items(limit=batch_size).items
    for item in items:
        url = item.get("url", "")
        content = item.get("content", "")
        if content:
            url_hash = generate_url_hash(url)
            destination = f"pending/{project_id}/{datetime.now().strftime('%Y%m%d')}/{url_hash}.html.gz"
            compress_and_upload(content, project_config["gcs_bucket"], destination)

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(customer_config["gcp_project_id"], "generate-xml")
    publisher.publish(topic_path, json.dumps({
        "customer": customer_id,
        "project": project_id,
        "dataset_id": dataset_id
    }).encode("utf-8"))

    return {"status": "success"}