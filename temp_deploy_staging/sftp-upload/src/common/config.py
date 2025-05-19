import json
import os
from google.cloud import secretmanager

def load_customer_config(customer_id):
    with open(f"src/configs/customers/{customer_id}.json") as f:
        return json.load(f)

def load_project_config(project_id):
    with open(f"src/configs/projects/{project_id}.json") as f:
        return json.load(f)

def get_secret(secret_id, project_id):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")