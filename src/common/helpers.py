import logging
import re
import json
from bs4 import BeautifulSoup
from datetime import datetime
try:
    from vertexai.generative_models import GenerativeModel
    import vertexai
except ImportError:
    GenerativeModel = None
    vertexai = None

def validate_html_content(content, logger_instance):
    """Validate and normalize HTML content encoding."""
    if not content:
        logger_instance.warning("Empty HTML content provided.")
        return None
    try:
        if isinstance(content, bytes):
            try:
                content = content.decode('utf-8')
            except UnicodeDecodeError:
                try:
                    content = content.decode('latin-1')
                    logger_instance.warning("HTML content decoded with latin-1 fallback.")
                except UnicodeDecodeError:
                    content = content.decode('utf-8', errors='replace')
                    logger_instance.warning("HTML content decoded with error replacement.")
        content.encode('utf-8')
        return content
    except Exception as e:
        logger_instance.error(f"Failed to validate HTML content: {str(e)}")
        return None

def find_url_in_item(item, logger_instance):
    """Search for a valid URL in the item, including nested fields."""
    url_fields = ['html_url', 'url', 'main_url', 'loaded_url', 'page_url', 'request_url', 'web_url']
    content_url = None
    found_field = None

    for field in url_fields:
        for variant in [field, field.lower(), field.upper(), field.capitalize(), field.replace('_', '')]:
            value = item.get(variant)
            if is_valid_url(value):
                content_url = value.strip()
                found_field = variant
                logger_instance.debug(f"Found valid URL in field '{variant}': {content_url}")
                return content_url, found_field

    for key, value in item.items():
        if isinstance(value, str) and is_valid_url(value):
            content_url = value.strip()
            found_field = key
            logger_instance.debug(f"Found valid URL in field '{key}': {content_url}")
            return content_url, found_field
        elif isinstance(value, dict):
            for sub_key, sub_value in value.items():
                if isinstance(sub_value, str) and is_valid_url(sub_value):
                    content_url = sub_value.strip()
                    found_field = f"{key}.{sub_key}"
                    logger_instance.debug(f"Found valid URL in nested field '{found_field}': {content_url}")
                    return content_url, found_field

    logger_instance.warning(f"No valid URL found in item. Available fields: {list(item.keys())}")
    return None, None

def is_valid_url(value):
    """Validate if a value is a valid URL."""
    if not isinstance(value, str) or not value.strip():
        return False
    return value.lower().startswith(('http://', 'https://')) and len(value.strip()) > 10

def resolve_computed_field(source, item, extra_context, logger_instance):
    """Resolve computed field values based on source expression."""
    if "generate_law_id" in source:
        if extra_context and "project" in extra_context and "sequence_number" in extra_context:
            return generate_law_id(extra_context["project"], extra_context["sequence_number"])
        logger_instance.warning("Missing project or sequence_number for generate_law_id.")
        return "Not Available"
    elif "extract_date_from_html" in source:
        content_fields = source.replace("extract_date_from_html(", "").replace(")", "").split(" || ")
        for content_field in content_fields:
            content_field = content_field.strip()
            if item.get(content_field):
                date = extract_date_from_html(item[content_field], logger_instance)
                if date != "Not Available":
                    return date
        logger_instance.warning(f"No valid date extracted from fields: {content_fields}")
        return "Not Available"
    elif "extract_title_from_html" in source:
        content_fields = source.replace("extract_title_from_html(", "").replace(")", "").split(" || ")
        for content_field in content_fields:
            content_field = content_field.strip()
            if item.get(content_field):
                title = extract_title_from_html(item[content_field], logger_instance)
                if title != "untitled":
                    return title
        logger_instance.warning(f"No valid title extracted from fields: {content_fields}")
        return "untitled"
    elif "split('/').last.replace('.html', '')" in source:
        url_field = source.split(".split")[0].strip()
        if item.get(url_field):
            try:
                return item[url_field].split("/")[-1].replace(".html", "")
            except Exception as e:
                logger_instance.warning(f"Error parsing URL for filename: {str(e)}")
                return ""
        return ""
    logger_instance.warning(f"Unsupported computed field source: {source}")
    return "Not Available"

def get_mapped_field(item, field, field_mappings, logger_instance=None, extra_context=None):
    """Get the value for a field based on its mapping in field_mappings."""
    current_logger = logger_instance if logger_instance else logging.getLogger(__name__)
    mapping = field_mappings.get(field, {})
    source = mapping.get("source", field)
    field_type = mapping.get("type", "direct")

    if field_type == "computed":
        value = resolve_computed_field(source, item, extra_context or {}, current_logger)
        if value == "Not Available" or value == "untitled":
            current_logger.debug(f"Computed field '{field}' returned default value: {value}")
        return value
    else:
        source_fields = source.split(" || ")
        for source_field in source_fields:
            source_field = source_field.strip()
            if item.get(source_field) is not None:
                value = item[source_field]
                if isinstance(value, str) and value.strip():
                    current_logger.debug(f"Found value for field '{field}' in source '{source_field}': {value}")
                    return value
        current_logger.warning(f"No valid value found for field '{field}' in sources: {source_fields}. Item keys: {list(item.keys())}")
        return "Not Available"

def generate_law_id(project_config_name, sequence_number, abbreviation=None):
    """Generate a unique Law-ID based on an abbreviation and sequence number."""
    logger_instance = logging.getLogger(__name__)
    try:
        if abbreviation:
            abbr = abbreviation
        else:
            if not project_config_name or not isinstance(project_config_name, str):
                logger_instance.warning(f"Invalid project_config_name: {project_config_name}. Using LLM fallback.")
                abbr = get_llm_abbreviation(project_config_name, logger_instance)
            else:
                words = project_config_name.split('_')
                abbr = ''.join(word[0].upper() for word in words if word)
                if not abbr or len(abbr) > 5 or len(abbr) < 2:
                    logger_instance.debug(f"Rule-based abbreviation '{abbr}' invalid. Using LLM fallback.")
                    abbr = get_llm_abbreviation(project_config_name, logger_instance)
                else:
                    logger_instance.debug(f"Rule-based abbreviation: {abbr}")

        safe_abbr = "".join(c if c.isalnum() else '_' for c in abbr)
        law_id = f"{safe_abbr}_{sequence_number:06d}"
        return law_id
    except Exception as e:
        logger_instance.error(f"Failed to generate Law-ID: {str(e)}", exc_info=True)
        return "Not Available"

def get_llm_abbreviation(project_config_name, logger_instance):
    """Use Vertex AI to generate a concise abbreviation for the project ID."""
    if not GenerativeModel or not vertexai:
        logger_instance.warning("Vertex AI not available. Using default abbreviation 'UNK'.")
        return 'UNK'

    try:
        vertexai.init(project="435549401163", location="europe-west1")
        model = GenerativeModel("gemini-2.0-flash-lite-001")
        prompt = f"""
        Given the project ID "{project_config_name}", generate a concise abbreviation (3-5 characters, uppercase, alphanumeric) by summarizing the meaning. For example, "united_states_federal_law" might become "USFL".

        Return the abbreviation as a plain string (e.g., GFL).
        """
        generation_config = {
            "temperature": 0.5,
            "max_output_tokens": 10,
            "top_p": 0.8,
            "top_k": 20
        }
        response = model.generate_content([prompt], generation_config=generation_config)
        if not response.candidates or not response.candidates[0].content.parts:
            logger_instance.warning("Vertex AI returned no response for abbreviation.")
            return 'UNK'

        abbr = response.candidates[0].content.parts[0].text.strip()
        if re.match(r'^[A-Z0-9]{3,5}$', abbr):
            logger_instance.info(f"LLM generated abbreviation: {abbr}")
            return abbr
        logger_instance.warning(f"LLM returned invalid abbreviation: {abbr}. Using default 'UNK'.")
        return 'UNK'
    except Exception as e:
        logger_instance.warning(f"LLM abbreviation generation failed: {str(e)}")
        return 'UNK'

def extract_title_from_html(html_content, logger_instance=None):
    """Extract the title from HTML content using configurable selectors."""
    current_logger = logger_instance if logger_instance else logging.getLogger(__name__)
    html_content = validate_html_content(html_content, current_logger)
    if not html_content:
        return "untitled"
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        selectors = [
            ('h1', {'class': 'jnlangue'}),
            ('h1', {}),
            ('title', {}),
            ('div', {'class': 'jnnorm'}),
            ('p', {'class': 'jntitel'})
        ]
        for tag, attrs in selectors:
            elem = soup.find(tag, **attrs)
            if elem:
                title = elem.get_text(strip=True)
                if title and len(title) > 5:
                    current_logger.debug(f"Extracted title using {tag} with attrs {attrs}: {title}")
                    return title
        current_logger.warning("No valid title found in HTML. Checking meta tags.")
        meta_title = soup.find('meta', {'name': 'title'})
        if meta_title and meta_title.get('content'):
            title = meta_title.get('content').strip()
            if title:
                current_logger.debug(f"Extracted title from meta tag: {title}")
                return title
        current_logger.debug("No title found in HTML or meta tags.")
        return "untitled"
    except Exception as e:
        current_logger.warning(f"Error extracting title from HTML: {str(e)}")
        return "untitled"

def extract_date_from_html(html_content, logger_instance=None):
    """Extract the Ausfertigungsdatum from HTML content using configurable patterns."""
    current_logger = logger_instance if logger_instance else logging.getLogger(__name__)
    html_content = validate_html_content(html_content, current_logger)
    if not html_content:
        return "Not Available"
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        patterns = [
            (r'Ausfertigungsdatum\s*:\s*([\d\.]+)', 'p'),
            (r'Date\s*:\s*([\d\.]+)', 'p'),
            (r'Issued\s*:\s*([\d\.]+)', 'p'),
            (r'Veröffentlicht\s*:\s*([\d\.]+)', 'p'),
            (r'([\d]{2}\.[\d]{2}\.[\d]{4})', 'div')
        ]
        for pattern, tag in patterns:
            elem = soup.find(tag, string=re.compile(pattern, re.I))
            if elem:
                date_text = re.search(pattern, elem.get_text(), re.I)
                if date_text:
                    date = date_text.group(1)
                    if re.match(r'\d{2}\.\d{2}\.\d{4}', date):
                        current_logger.debug(f"Extracted date using pattern {pattern} in tag {tag}: {date}")
                        return date
        current_logger.warning("No valid date found in HTML.")
        return "Not Available"
    except Exception as e:
        current_logger.warning(f"Error extracting Ausfertigungsdatum from HTML: {str(e)}")
        return "Not Available"

def sanitize_field_name(field_name):
    """Sanitize Firestore field names by replacing invalid characters, preserving specific fields."""
    if not field_name:
        return field_name
    if field_name in ["Law-ID", "Ausfertigungsdatum"]:  # Preserve specific field names
        return field_name
    return re.sub(r'[^\w\-]', '_', field_name)  # Allow hyphens

def sanitize_error_message(error_message):
    """Sanitize an error message to remove sensitive information."""
    try:
        sanitized = re.sub(r'(?i)(api_key|token|password|secret)=[^& ]+', r'\1=REDACTED', error_message)
        sanitized = re.sub(r'[^\w\s.,:;-]', '_', sanitized)
        return sanitized[:1000]
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.warning(f"Error sanitizing error message: {str(e)}")
        return "Error message sanitization failed"

def analyze_error_with_vertex_ai(error_message, stage, field_mappings, dataset_fields, gcp_project_id, logger_instance=None, extra_context=None):
    """Analyze an error message using Vertex AI to determine retry strategy."""
    current_logger = logger_instance if logger_instance else logging.getLogger(__name__)
    
    if not GenerativeModel or not gcp_project_id:
        current_logger.warning("Vertex AI not available or GCP project ID missing. Skipping error analysis.")
        return {
            "retry": False,
            "reason": "Vertex AI not configured",
            "category": "configuration_error",
            "adjusted_params": {}
        }

    try:
        vertexai.init(project=gcp_project_id, location="europe-west1")
        model = GenerativeModel("gemini-2.0-flash-lite-001")
        doc_data = extra_context.get("doc_data", {}) if extra_context else {}
        doc_fields = list(doc_data.keys()) if isinstance(doc_data, dict) else []
        prompt = f"""
        Analyze the following error from a data processing pipeline at stage '{stage}':

        Error Message:
        ```
        {error_message}
        ```

        Context:
        - Pipeline stage: {stage}
        - Available dataset fields: {dataset_fields}
        - Configured field mappings: {json.dumps(field_mappings, indent=2)}
        - Firestore document fields: {doc_fields}

        Determine if the error is retryable, categorize the error type (e.g., network_error, data_error, configuration_error, permission_error, timeout_error), and suggest adjustments for retry (e.g., truncate_content, recheck_metadata, field_remapping).

        Return a JSON object with:
        - retry: Boolean indicating if the error should be retried
        - reason: Explanation of the retry decision
        - category: Error category
        - adjusted_params: Dictionary with retry adjustments (e.g., {{"truncate_content": true, "field_remapping": {{"field": "new_source"}}}})

        Example:
        ```json
        {{
            "retry": true,
            "reason": "Temporary network issue detected",
            "category": "network_error",
            "adjusted_params": {{"truncate_content": false, "recheck_metadata": true, "field_remapping": {{}}}}
        }}
        ```
        """
        generation_config = {
            "temperature": 0.5,
            "max_output_tokens": 300,
            "top_p": 0.8,
            "top_k": 20
        }
        response = model.generate_content([prompt], generation_config=generation_config)
        if not response.candidates or not response.candidates[0].content.parts:
            current_logger.warning("Vertex AI returned no predictions for error analysis.")
            return {
                "retry": False,
                "reason": "No response from Vertex AI",
                "category": "llm_error",
                "adjusted_params": {}
            }
        
        prediction_content = response.candidates[0].content.parts[0].text
        match = re.search(r"```json\s*([\s\S]*?)\s*```", prediction_content, re.MULTILINE | re.DOTALL)
        json_str = match.group(1).strip() if match else prediction_content.strip()
        try:
            result = json.loads(json_str)
            required_keys = ["retry", "reason", "category", "adjusted_params"]
            if not all(key in result for key in required_keys):
                current_logger.warning(f"Invalid Vertex AI response format: {json_str}")
                return {
                    "retry": False,
                    "reason": "Invalid response format from Vertex AI",
                    "category": "llm_error",
                    "adjusted_params": {}
                }
            current_logger.info(f"Vertex AI error analysis: {result}")
            return result
        except json.JSONDecodeError:
            current_logger.warning(f"Vertex AI response was not valid JSON: {prediction_content}")
            return {
                "retry": False,
                "reason": "Invalid JSON response from Vertex AI",
                "category": "llm_error",
                "adjusted_params": {}
            }
    except Exception as e:
        current_logger.warning(f"Vertex AI error analysis failed: {str(e)}", exc_info=True)
        return {
            "retry": False,
            "reason": f"Vertex AI error analysis failed: {str(e)}",
            "category": "llm_error",
            "adjusted_params": {}
        }

def serialize_firestore_doc(data):
    """Convert Firestore document data to JSON-serializable format."""
    if isinstance(data, dict):
        return {k: serialize_firestore_doc(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [serialize_firestore_doc(item) for item in data]
    elif isinstance(data, datetime):
        return data.isoformat()
    return data