import json
import os
from google.cloud import secretmanager

def load_customer_config(customer_id):
    """Load customer configuration from src/configs/customers/{customer_id}.json."""
    file_path = f"src/configs/customers/{customer_id}.json"
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Customer configuration file not found: {file_path}")
    with open(file_path) as f:
        return json.load(f)

def load_project_config(project_id):
    """Load project configuration from src/configs/projects/{project_id}.json."""
    file_path = f"src/configs/projects/{project_id}.json"
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Project configuration file not found: {file_path}")
    with open(file_path) as f:
        return json.load(f)

def get_secret(secret_id, project_id):
    """Retrieve a secret from Google Cloud Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    try:
        response = client.access_secret_version(name=name)
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        raise RuntimeError(f"Failed to access secret {secret_id} in project {project_id}: {str(e)}")