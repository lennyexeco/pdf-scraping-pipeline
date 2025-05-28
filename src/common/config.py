import json
import os
from google.cloud.secretmanager import SecretManagerServiceClient


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
    
# In src/common/config.py (or a new config_loader.py)
def load_dynamic_site_config(db, project_config_name, logger_instance):
    """
    Loads the AI-generated site schema from Firestore,
    and merges it with or falls back to static project config.
    """
    site_schema = None
    try:
        project_doc_ref = db.collection("projects").document(project_config_name)
        doc = project_doc_ref.get()
        if doc.exists:
            data = doc.to_dict()
            if "metadata_schema" in data: # This is where analyze_website_schema stores it
                site_schema = data["metadata_schema"]
                logger_instance.info(f"Successfully loaded AI-generated schema for project {project_config_name} from Firestore.")
            else:
                logger_instance.warning(f"No 'metadata_schema' field in Firestore for project {project_config_name}.")
        else:
            logger_instance.warning(f"No Firestore document found for project {project_config_name} to load AI schema.")
    except Exception as e:
        logger_instance.error(f"Error loading AI schema from Firestore for {project_config_name}: {e}")

    # Load base static config (e.g., for global settings, fallback values)
    static_config = load_project_config(project_config_name) # Your existing function

    if site_schema:
        # Merge dynamic schema with static config.
        # Dynamic schema should take precedence for overlapping keys it's designed to override.
        # Example of a simple merge (you might need a more sophisticated deep merge):
        merged_config = {**static_config, **site_schema}
        # Specifically, ensure nested structures like xml_structure, field_mappings
        # from site_schema are used if present.
        if "xml_structure" in site_schema:
            merged_config["xml_structure"] = site_schema["xml_structure"]
        if "field_mappings" in site_schema:
            merged_config["field_mappings"] = site_schema["field_mappings"]
        if "fallback_selectors" in site_schema: # From analyze_website_schema output
            merged_config["discovery_selectors"] = site_schema["fallback_selectors"] # Rename for clarity
        if "metadata_fields" in site_schema:
             merged_config["metadata_extraction_fields_config"] = site_schema["metadata_fields"]
        if "language_and_country" in site_schema:
            merged_config["language_and_country"] = site_schema["language_and_country"]

        logger_instance.info(f"Using merged configuration for {project_config_name} (AI-schema + static).")
        return merged_config
    else:
        logger_instance.warning(f"AI-generated schema not available for {project_config_name}. Using only static configuration.")
        return static_config

def get_secret(secret_id, project_id):
    """Retrieve a secret from Google Cloud Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    try:
        response = client.access_secret_version(name=name)
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        raise RuntimeError(f"Failed to access secret {secret_id} in project {project_id}: {str(e)}")