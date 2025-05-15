from google.cloud import firestore, pubsub_v1
from apify_client import ApifyClient
from src.common.config import load_customer_config, load_project_config, get_secret
from src.common.utils import generate_url_hash, logger

def extract_metadata(event, context):
    data = json.loads(event['data'].decode('utf-8'))
    customer_id = data['customer']
    project_id = data['project']
    dataset_id = data['dataset_id']

    customer_config = load_customer_config(customer_id)
    project_config = load_project_config(project_id)
    apify_key = get_secret(customer_config['apify_api_key_secret'], customer_config['gcp_project_id'])

    client = ApifyClient(apify_key)
    db = firestore.Client(project=customer_config['gcp_project_id'], database=customer_config['firestore_database'])

    items = client.dataset(dataset_id).list_items(limit=50).items
    for item in items:
        url = item.get('url', '')
        url_hash = generate_url_hash(url)
        doc_ref = db.collection(project_config['firestore_collection']).document(url_hash)
        doc_ref.set({
            'url': url,
            'title': item.get('title', 'Not Found'),
            'description': item.get('Inhoudsindicatie', ''),
            'scrape_date': firestore.SERVER_TIMESTAMP
        })
        logger.info(f"Saved metadata for {url}")

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(customer_config['gcp_project_id'], 'store-html')
    publisher.publish(topic_path, json.dumps({
        'customer': customer_id,
        'project': project_id,
        'dataset_id': dataset_id
    }).encode('utf-8'))