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


def load_dynamic_site_config(db, project_config_name, logger_instance):
    """
    Load project configuration from Firestore site_configs collection.
    This is the FEDAO-compatible version that loads static configs from Firestore.
    
    Args:
        db: Firestore database client
        project_config_name (str): Project configuration name (e.g., 'fedao_project')
        logger_instance: Logger instance
        
    Returns:
        dict: Project configuration
    """
    try:
        # Load from site_configs collection (where setup_config.sh uploads)
        config_doc_ref = db.collection("site_configs").document(project_config_name)
        doc = config_doc_ref.get()
        
        if doc.exists:
            config = doc.to_dict()
            logger_instance.info(f"Successfully loaded configuration for {project_config_name} from Firestore site_configs collection.")
            return config
        else:
            logger_instance.error(f"No configuration document found for {project_config_name} in site_configs collection.")
            raise FileNotFoundError(f"Project configuration not found in Firestore: {project_config_name}")
            
    except Exception as e:
        logger_instance.error(f"Error loading configuration from Firestore for {project_config_name}: {e}")
        raise


def deep_merge_config(base_config, override_config):
    """
    Deep merge two configuration dictionaries.
    Override config takes precedence for conflicting keys.
    
    Args:
        base_config (dict): Base configuration
        override_config (dict): Configuration to merge/override with
        
    Returns:
        dict: Merged configuration
    """
    if not isinstance(base_config, dict) or not isinstance(override_config, dict):
        return override_config if override_config is not None else base_config
    
    merged = base_config.copy()
    
    for key, value in override_config.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = deep_merge_config(merged[key], value)
        else:
            merged[key] = value
    
    return merged


def get_secret(secret_id, project_id):
    """Retrieve a secret from Google Cloud Secret Manager."""
    client = SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    try:
        response = client.access_secret_version(name=name)
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        raise RuntimeError(f"Failed to access secret {secret_id} in project {project_id}: {str(e)}")


def get_config_value(config, key_path, default_value=None):
    """
    Get a configuration value using dot notation (e.g., 'ai_model_config.discovery_model_id').
    
    Args:
        config (dict): Configuration dictionary
        key_path (str): Dot-separated key path
        default_value: Default value if key not found
        
    Returns:
        Any: Configuration value or default
    """
    try:
        value = config
        for key in key_path.split('.'):
            value = value[key]
        return value
    except (KeyError, TypeError):
        return default_value


def update_config_in_firestore(db, project_config_name, config_updates, logger_instance):
    """
    Update specific configuration values in Firestore.
    
    Args:
        db: Firestore database client
        project_config_name (str): Project configuration name
        config_updates (dict): Configuration updates to apply
        logger_instance: Logger instance
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        project_doc_ref = db.collection("site_configs").document(project_config_name)
        project_doc_ref.update({
            **config_updates,
            "config_updated_timestamp": db.SERVER_TIMESTAMP
        })
        
        logger_instance.info(f"Updated configuration for {project_config_name} in Firestore: {list(config_updates.keys())}")
        return True
        
    except Exception as e:
        logger_instance.error(f"Failed to update configuration for {project_config_name}: {e}")
        return False


# FEDAO-specific helper functions
def validate_fedao_config(config, logger_instance):
    """
    Validate FEDAO-specific configuration requirements.
    
    Args:
        config (dict): FEDAO project configuration
        logger_instance: Logger instance
        
    Returns:
        bool: True if valid, False otherwise
    """
    required_fedao_keys = [
        'fedao_input_configs',
        'field_mappings',
        'sequential_id_config',
        'firestore_collection'
    ]
    
    missing_keys = [key for key in required_fedao_keys if key not in config]
    
    if missing_keys:
        logger_instance.error(f"Missing required FEDAO configuration keys: {missing_keys}")
        return False
    
    # Validate fedao_input_configs structure
    input_configs = config.get('fedao_input_configs', {})
    expected_input_types = ['FEDAO_MOA_RAW_DATA', 'FEDAO_TOA_RAW_DATA']
    
    for input_type in expected_input_types:
        if input_type not in input_configs:
            logger_instance.warning(f"Missing FEDAO input config for: {input_type}")
        else:
            input_config = input_configs[input_type]
            required_input_keys = ['transformations', 'gcs_processed_path_template', 'output_document_type']
            missing_input_keys = [key for key in required_input_keys if key not in input_config]
            if missing_input_keys:
                logger_instance.error(f"Missing keys in {input_type} config: {missing_input_keys}")
                return False
    
    logger_instance.info("FEDAO configuration validation passed")
    return True


def get_fedao_input_config(config, csv_filename, logger_instance):
    """
    Get the appropriate FEDAO input configuration based on CSV filename.
    
    Args:
        config (dict): FEDAO project configuration
        csv_filename (str): Name of the CSV file (e.g., 'FEDAO_MOA_RAW_DATA.csv')
        logger_instance: Logger instance
        
    Returns:
        dict: Input configuration for the specific CSV type
    """
    # Remove .csv extension to get config key
    config_key = os.path.splitext(csv_filename)[0]
    
    fedao_configs = config.get('fedao_input_configs', {})
    
    if config_key in fedao_configs:
        logger_instance.info(f"Found FEDAO input config for: {config_key}")
        return fedao_configs[config_key]
    else:
        logger_instance.error(f"No FEDAO input configuration found for: {config_key}")
        logger_instance.debug(f"Available config keys: {list(fedao_configs.keys())}")
        return None