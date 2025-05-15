from google.cloud import pubsub_v1
from apify_client import ApifyClient
from src.common.config import load_customer_config, load_project_config, get_secret
from src.common.utils import compress_and_upload, generate_url_hash, logger

def store_html(event, context):
    data = json.loads(event['data'].decode('utf-8'))
    customer_id = data['customer']
    project_id = data['project']
    dataset_id = data['dataset_id']

    customer_config = load_customer_config(customer_id)
    project_config = load_project_config(project_id)
    apify_key = get_secret(customer_config['apify_api_key_secret'], customer_config['gcp_project_id'])

    client = ApifyClient(apify_key)
    items = client.dataset(dataset_id).list_items(limit=50).items
    for item in items:
        url = item.get('url', '')
        content = item.get('content', '')
        if content:
            url_hash = generate_url_hash(url)
            destination = f"pending/{project_id}/{datetime.now().strftime('%Y%m%d')}/{url_hash}.html.gz"
            compress_and_upload(content, project_config['gcs_bucket'], destination)

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(customer_config['gcp_project_id'], 'generate-xml')
    publisher.publish(topic_path, json.dumps({
        'customer': customer_id,
        'project': project_id,
        'dataset_id': dataset_id
    }).encode('utf-8'))