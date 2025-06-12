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


def normalize_ai_schema_keys(ai_schema):
    """
    Normalize AI-generated schema keys to match expected pipeline configuration keys.
    
    Args:
        ai_schema (dict): AI-generated schema from analyze_website_schema
        
    Returns:
        dict: Normalized schema with standard key names
    """
    normalized = {}
    
    # Map AI schema keys to pipeline configuration keys
    key_mappings = {
        # AI schema key -> Pipeline config key
        'fallback_selectors': 'discovery_selectors',
        'discovery_selectors': 'discovery_selectors',  # In case AI uses the right key
        'metadata_fields': 'ai_extraction_fields_config',
        'heuristic_link_extraction': 'heuristic_link_extraction',
        'document_type_patterns': 'document_classification_patterns',
        'language_and_country': 'language_and_country',
        'field_mappings': 'field_mappings',
        'xml_structure': 'xml_structure'
    }
    
    for ai_key, pipeline_key in key_mappings.items():
        if ai_key in ai_schema:
            normalized[pipeline_key] = ai_schema[ai_key]
    
    # Handle nested discovery_selectors structure
    if 'discovery_selectors' in normalized:
        discovery_config = normalized['discovery_selectors']
        
        # Ensure discovery_selectors has the right structure
        if isinstance(discovery_config, dict):
            # Normalize selector keys
            if 'main_url_selectors' in discovery_config:
                # Ensure it's a list
                if not isinstance(discovery_config['main_url_selectors'], list):
                    discovery_config['main_url_selectors'] = [discovery_config['main_url_selectors']]
            
            if 'pagination_selector' in discovery_config:
                # Keep as single selector (not a list)
                if isinstance(discovery_config['pagination_selector'], list):
                    discovery_config['pagination_selector'] = discovery_config['pagination_selector'][0] if discovery_config['pagination_selector'] else None
    
    # Handle heuristic configuration
    if 'heuristic_link_extraction' in normalized:
        heuristic_config = normalized['heuristic_link_extraction']
        
        # Ensure required fields have defaults
        if isinstance(heuristic_config, dict):
            heuristic_config.setdefault('enabled', True)
            heuristic_config.setdefault('discovery_threshold', 1)
            heuristic_config.setdefault('min_link_text_len', 5)
    
    return normalized


def validate_merged_config(config, project_config_name, logger_instance):
    """
    Validate the merged configuration has required fields and sensible values.
    
    Args:
        config (dict): Merged configuration to validate
        project_config_name (str): Project name for logging
        logger_instance: Logger instance
        
    Returns:
        dict: Validated configuration with defaults added if needed
    """
    # Required top-level fields
    required_fields = {
        'gcs_bucket': None,
        'firestore_collection': 'processedUrls',
        'project_abbreviation': 'DOC',
        'vertex_ai_location': 'europe-west1'
    }
    
    for field, default_value in required_fields.items():
        if field not in config and default_value is not None:
            config[field] = default_value
            logger_instance.warning(f"Added default value for {field}: {default_value}")
    
    # Validate discovery configuration
    if 'discovery_selectors' not in config:
        config['discovery_selectors'] = {
            'main_url_selectors': ['article a', 'h2 a', 'h3 a'],
            'pagination_selector': None
        }
        logger_instance.warning(f"Added default discovery_selectors for {project_config_name}")
    
    # Validate heuristic configuration
    if 'heuristic_link_extraction' not in config:
        config['heuristic_link_extraction'] = {
            'enabled': True,
            'content_selectors': ['main', 'article', '.content'],
            'exclusion_selectors': ['nav', 'footer', '.breadcrumb'],
            'link_text_keywords': ['report', 'guide', 'document'],
            'href_patterns': [r'\.pdf$'],
            'min_link_text_len': 5,
            'discovery_threshold': 1
        }
        logger_instance.warning(f"Added default heuristic_link_extraction for {project_config_name}")
    
    # Validate AI model configuration
    if 'ai_model_config' not in config:
        config['ai_model_config'] = {
            'discovery_model_id': 'gemini-2.0-flash-001',
            'metadata_extraction_model_id': 'gemini-2.0-flash-001',
            'schema_analysis_model_id': 'gemini-2.0-flash-001'
        }
        logger_instance.warning(f"Added default ai_model_config for {project_config_name}")
    
    # Validate XML structure
    if 'xml_structure' not in config:
        config['xml_structure'] = {
            'root_tag': 'Document',
            'filename_template': '{indexed_filename_base}.xml',
            'gcs_xml_output_path': 'xml_output/{project_id_sanitized}/{date_str}',
            'declaration': True,
            'indent': '  ',
            'fields': [
                {'tag': 'DocumentID', 'source': 'indexed_filename_base', 'cdata': False},
                {'tag': 'Title', 'source': 'Document_Title', 'cdata': False},
                {'tag': 'Content', 'source': 'document_content', 'cdata': True}
            ]
        }
        logger_instance.warning(f"Added default xml_structure for {project_config_name}")
    
    # Validate document classification patterns
    if 'document_classification_patterns' not in config:
        config['document_classification_patterns'] = {
            'Report': ['report', 'rep-'],
            'Guide': ['guide', 'rg-'],
            'Media Release': ['media release', 'mr-'],
            'Consultation': ['consultation', 'cp-']
        }
        logger_instance.info(f"Added default document_classification_patterns for {project_config_name}")
    
    return config


def load_dynamic_site_config(db, project_config_name, logger_instance):
    """
    Enhanced function to load AI-generated site schema from Firestore
    and merge it with static project config using proper deep merging.
    
    Args:
        db: Firestore database client
        project_config_name (str): Project configuration name
        logger_instance: Logger instance
        
    Returns:
        dict: Merged and validated configuration
    """
    ai_schema = None
    
    # Try to load AI-generated schema from Firestore
    try:
        project_doc_ref = db.collection("projects").document(project_config_name)
        doc = project_doc_ref.get()
        
        if doc.exists:
            data = doc.to_dict()
            if "metadata_schema" in data:
                ai_schema = data["metadata_schema"]
                schema_timestamp = data.get("schema_generated_timestamp")
                logger_instance.info(f"Successfully loaded AI-generated schema for project {project_config_name} from Firestore (generated: {schema_timestamp}).")
            else:
                logger_instance.warning(f"No 'metadata_schema' field in Firestore for project {project_config_name}.")
        else:
            logger_instance.warning(f"No Firestore document found for project {project_config_name} to load AI schema.")
            
    except Exception as e:
        logger_instance.error(f"Error loading AI schema from Firestore for {project_config_name}: {e}")

    # Load base static configuration
    try:
        static_config = load_project_config(project_config_name)
        logger_instance.info(f"Loaded static configuration for {project_config_name}")
    except FileNotFoundError as e:
        logger_instance.error(f"Static configuration not found for {project_config_name}: {e}")
        # Create minimal default config
        static_config = {
            'gcs_bucket': None,  # Must be provided
            'firestore_collection': 'processedUrls',
            'project_abbreviation': project_config_name.upper()[:10]
        }

    # Merge configurations if AI schema is available
    if ai_schema:
        logger_instance.info(f"Merging AI-generated schema with static config for {project_config_name}")
        
        # Normalize AI schema keys
        normalized_ai_schema = normalize_ai_schema_keys(ai_schema)
        logger_instance.debug(f"Normalized AI schema keys: {list(normalized_ai_schema.keys())}")
        
        # Deep merge with AI schema taking precedence
        merged_config = deep_merge_config(static_config, normalized_ai_schema)
        
        # Add metadata about the AI schema
        merged_config['_ai_schema_metadata'] = {
            'schema_available': True,
            'schema_timestamp': data.get("schema_generated_timestamp") if 'data' in locals() else None,
            'samples_analyzed': data.get("schema_analysis_samples_count") if 'data' in locals() else None
        }
        
        logger_instance.info(f"Successfully merged AI schema with static config for {project_config_name}")
        
    else:
        merged_config = static_config.copy()
        merged_config['_ai_schema_metadata'] = {'schema_available': False}
        logger_instance.warning(f"AI-generated schema not available for {project_config_name}. Using only static configuration.")

    # Validate and add defaults to the merged configuration
    final_config = validate_merged_config(merged_config, project_config_name, logger_instance)
    
    # Log the final configuration structure (without sensitive data)
    config_summary = {
        'ai_schema_available': final_config.get('_ai_schema_metadata', {}).get('schema_available', False),
        'discovery_method': 'AI+Heuristic' if ai_schema else 'Heuristic+Fallback',
        'has_xml_structure': 'xml_structure' in final_config,
        'has_field_mappings': 'field_mappings' in final_config,
        'heuristic_enabled': final_config.get('heuristic_link_extraction', {}).get('enabled', False)
    }
    logger_instance.info(f"Final configuration summary for {project_config_name}: {config_summary}")
    
    return final_config


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
        project_doc_ref = db.collection("projects").document(project_config_name)
        project_doc_ref.update({
            **config_updates,
            "config_updated_timestamp": db.SERVER_TIMESTAMP
        })
        
        logger_instance.info(f"Updated configuration for {project_config_name} in Firestore: {list(config_updates.keys())}")
        return True
        
    except Exception as e:
        logger_instance.error(f"Failed to update configuration for {project_config_name}: {e}")
        return False