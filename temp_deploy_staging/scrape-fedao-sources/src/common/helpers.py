import logging
import re
import json
from datetime import datetime


def sanitize_field_name(field_name):
    """
    Sanitize Firestore field names by replacing invalid characters.
    Preserves specific FEDAO field names that are commonly used.
    """
    if not field_name:
        return field_name
    
    # Preserve specific FEDAO field names
    preserved_fields = [
        "Operation_Type", "Security_Type", "Period", "Release_Date",
        "Maximum_Operation_Currency", "Maximum_Operation_Size", 
        "Maximum_Operation_Multiplier", "CUSIP", "Security_Maximum"
    ]
    
    if field_name in preserved_fields:
        return field_name
    
    # Replace invalid Firestore characters with underscores
    # Firestore field names cannot contain: . [ ] * /
    sanitized = re.sub(r'[.\[\]*\/]', '_', field_name)
    
    # Replace multiple underscores with single underscore
    sanitized = re.sub(r'_+', '_', sanitized)
    
    # Remove leading/trailing underscores
    sanitized = sanitized.strip('_')
    
    return sanitized if sanitized else 'unknown_field'


def validate_csv_content(content, logger_instance):
    """
    Validate CSV content for basic formatting issues.
    
    Args:
        content (str): Raw CSV content
        logger_instance: Logger instance
        
    Returns:
        str: Validated content or None if invalid
    """
    if not content:
        logger_instance.warning("Empty CSV content provided.")
        return None
    
    try:
        # Basic CSV validation
        lines = content.strip().split('\n')
        
        if len(lines) < 2:
            logger_instance.warning("CSV content has fewer than 2 lines (header + data).")
            return None
        
        # Check if first line looks like headers
        header_line = lines[0]
        if ',' not in header_line and '\t' not in header_line:
            logger_instance.warning("CSV header line doesn't appear to contain separators.")
            return None
        
        logger_instance.debug(f"CSV validation passed. {len(lines)} lines, header: {header_line[:100]}...")
        return content
        
    except Exception as e:
        logger_instance.error(f"Failed to validate CSV content: {str(e)}")
        return None


def extract_fedao_date_range(date_string):
    """
    Extract the larger date from Federal Reserve date ranges.
    Examples: "7/15/2024 - 8/13/2024" -> "8/13/2024"
              "July 29 - August 13, 2024" -> "August 13, 2024"
    
    Args:
        date_string (str): Date string that may contain a range
        
    Returns:
        str: The larger/end date from the range, or original string if no range
    """
    if not date_string or not isinstance(date_string, str):
        return date_string
    
    # Look for date ranges with hyphens or "to"
    if ' - ' in date_string or ' to ' in date_string:
        # Split on common range separators
        separator = ' - ' if ' - ' in date_string else ' to '
        parts = date_string.split(separator)
        
        if len(parts) == 2:
            # Return the second (larger) date
            return parts[1].strip()
    
    # If no range found, return original
    return date_string.strip()


def parse_fedao_amount(amount_string):
    """
    Parse Federal Reserve monetary amounts into components.
    Example: "$80 million" -> {"currency": "$", "amount": "80", "multiplier": "million"}
    
    Args:
        amount_string (str): String containing monetary amount
        
    Returns:
        dict: Dictionary with currency, amount, and multiplier components
    """
    if not amount_string or not isinstance(amount_string, str):
        return {"currency": "", "amount": "", "multiplier": ""}
    
    # Remove extra whitespace
    cleaned = amount_string.strip()
    
    # Pattern to match currency symbol, number, and multiplier
    pattern = r'([\$€£¥]?)?\s*([\d,]+(?:\.\d+)?)\s*([a-zA-Z]+)?'
    match = re.search(pattern, cleaned)
    
    if match:
        currency = match.group(1) or ""
        amount = match.group(2).replace(',', '') if match.group(2) else ""
        multiplier = match.group(3) or ""
        
        # Standardize multiplier spelling
        if multiplier.lower() in ['millions', 'million', 'mil', 'm']:
            multiplier = 'million'
        elif multiplier.lower() in ['billions', 'billion', 'bil', 'b']:
            multiplier = 'billion'
        elif multiplier.lower() in ['trillions', 'trillion', 'tril', 't']:
            multiplier = 'trillion'
        
        return {
            "currency": currency,
            "amount": amount,
            "multiplier": multiplier
        }
    
    return {"currency": "", "amount": "", "multiplier": ""}


def split_multiline_cell_content(cell_content, separator='\n'):
    """
    Split cell content that contains multiple items on separate lines.
    Used for FEDAO TOA data where CUSIPs and amounts may be on multiple lines.
    
    Args:
        cell_content (str): Cell content that may contain multiple lines
        separator (str): Separator to split on (default: newline)
        
    Returns:
        list: List of non-empty items
    """
    if not cell_content or not isinstance(cell_content, str):
        return []
    
    # Split and clean up items
    items = []
    for item in cell_content.split(separator):
        cleaned_item = item.strip()
        if cleaned_item:  # Only include non-empty items
            items.append(cleaned_item)
    
    return items


def normalize_fedao_field_value(value, field_name=""):
    """
    Normalize field values for consistent storage in Firestore.
    
    Args:
        value: The value to normalize
        field_name (str): Name of the field (for context-specific normalization)
        
    Returns:
        Normalized value
    """
    if value is None:
        return ""
    
    if not isinstance(value, str):
        return str(value)
    
    # Basic cleanup
    normalized = value.strip()
    
    # Field-specific normalization
    if 'date' in field_name.lower() or 'period' in field_name.lower():
        # For date fields, try to extract the larger date if it's a range
        normalized = extract_fedao_date_range(normalized)
    
    elif 'operation' in field_name.lower() and 'size' in field_name.lower():
        # This is likely a monetary amount field
        # We don't parse it here, just clean it up
        normalized = re.sub(r'\s+', ' ', normalized)  # Normalize whitespace
    
    elif 'cusip' in field_name.lower():
        # CUSIP fields should be uppercase and clean
        normalized = normalized.upper().replace(' ', '')
    
    return normalized


def serialize_firestore_doc(data):
    """
    Convert Firestore document data to JSON-serializable format.
    Handles datetime objects and other special types.
    """
    if isinstance(data, dict):
        return {k: serialize_firestore_doc(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [serialize_firestore_doc(item) for item in data]
    elif isinstance(data, datetime):
        return data.isoformat()
    elif hasattr(data, '__dict__'):
        # Handle objects with attributes
        return serialize_firestore_doc(data.__dict__)
    else:
        return data


def sanitize_filename(name, fallback_id=None, logger_instance=None):
    """
    Sanitize a string to be a safe filename for GCS storage.
    
    Args:
        name (str): Original filename/identifier
        fallback_id (str): Fallback if name is empty
        logger_instance: Logger instance
        
    Returns:
        str: Sanitized filename
    """
    if not name:
        name = fallback_id or "unknown_file"
    
    # Replace problematic characters with underscores
    sanitized = re.sub(r'[^\w\-_\.]', '_', str(name))
    
    # Remove consecutive underscores
    sanitized = re.sub(r'_+', '_', sanitized)
    
    # Remove leading/trailing underscores and dots
    sanitized = sanitized.strip('_.')
    
    # Ensure not empty
    if not sanitized:
        sanitized = fallback_id or "file"
    
    # Limit length
    if len(sanitized) > 100:
        sanitized = sanitized[:100].rstrip('_.')
    
    if logger_instance:
        logger_instance.debug(f"Sanitized filename: '{name}' -> '{sanitized}'")
    
    return sanitized


def log_fedao_operation(logger_instance, operation, status, **details):
    """
    Log FEDAO operations in a structured way for better monitoring.
    
    Args:
        logger_instance: Logger instance
        operation (str): Operation name (e.g., 'csv_transform', 'metadata_extract')
        status (str): Status (e.g., 'started', 'completed', 'failed')
        **details: Additional details to log
    """
    details_str = ", ".join([f"{k}={v}" for k, v in details.items()])
    log_message = f"FEDAO {operation} {status}"
    
    if details_str:
        log_message += f" - {details_str}"
    
    if status.lower() in ['failed', 'error']:
        logger_instance.error(log_message)
    elif status.lower() in ['warning', 'warn']:
        logger_instance.warning(log_message)
    else:
        logger_instance.info(log_message)
        
def validate_html_content(html_bytes_or_string, logger_instance, min_length=100):
    """
    Validates if the HTML content seems reasonable.
    Converts bytes to string if necessary.
    """
    active_logger = logger_instance or logging.getLogger(__name__)
    if not html_bytes_or_string:
        active_logger.warning("HTML content is empty or None.")
        return None

    html_string = ""
    if isinstance(html_bytes_or_string, bytes):
        try:
            # Try common encodings
            html_string = html_bytes_or_string.decode('utf-8')
        except UnicodeDecodeError:
            try:
                html_string = html_bytes_or_string.decode('latin-1') # Fallback
            except UnicodeDecodeError as e:
                active_logger.error(f"Could not decode HTML content: {e}")
                return None
    elif isinstance(html_bytes_or_string, str):
        html_string = html_bytes_or_string
    else:
        active_logger.error(f"Unexpected HTML content type: {type(html_bytes_or_string)}")
        return None

    if len(html_string) < min_length:
        active_logger.warning(f"HTML content is too short (length: {len(html_string)}, min: {min_length}).")
        return None

    # Basic check for HTML tags (very simplistic)
    if not ("<html" in html_string.lower() or "<body" in html_string.lower() or "<div" in html_string.lower()):
        active_logger.warning("HTML content lacks common HTML tags.")
        # Depending on strictness, you might return None here
        # For now, let's allow it if it passed length check

    active_logger.debug("HTML content validation passed basic checks.")
    return html_string

def sanitize_error_message(error_message, max_length=500):
    """
    Sanitizes an error message for safe logging or storage.
    Removes potentially sensitive info or excessive length.
    """
    if not error_message:
        return ""

    # Convert to string just in case
    msg_str = str(error_message)

    # Truncate
    if len(msg_str) > max_length:
        msg_str = msg_str[:max_length] + "..."

    # Remove newlines and carriage returns for single-line logging
    msg_str = msg_str.replace('\n', ' ').replace('\r', '')

    # Potentially remove or mask sensitive patterns (e.g., API keys, PII)
    # This requires careful regex and is context-dependent.
    # For a generic sanitizer, simple truncation and newline removal is a start.
    # Example (very basic, needs refinement for real use cases):
    # msg_str = re.sub(r'api_key=\w+', 'api_key=***MASKED***', msg_str, flags=re.IGNORECASE)

    return msg_str