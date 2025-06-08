import gzip
import hashlib
import logging
import re
from datetime import datetime
from google.cloud import storage
from google.cloud import logging as gcp_logging
import csv
from io import StringIO
import pandas as pd

def compress_and_upload(content, bucket_name, destination_blob_name, storage_client=None, content_type="text/html"):
    """Compress and upload content to GCS.

    Args:
        content (str): The content to compress and upload.
        bucket_name (str): The name of the GCS bucket.
        destination_blob_name (str): The destination path in the bucket.
        storage_client (google.cloud.storage.Client, optional): An initialized storage client. If None, a new client is created.
        content_type (str, optional): The MIME type of the content. Defaults to "text/html".

    Returns:
        str: The destination blob name.
    """
    op_logger = logging.getLogger(f"{__name__}.compress_and_upload")
    if storage_client is None:
        storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    compressed_content = gzip.compress(content.encode('utf-8'))
    blob.upload_from_string(compressed_content, content_type=content_type)
    op_logger.debug(f"Uploaded to gs://{bucket_name}/{destination_blob_name}")
    return destination_blob_name


def generate_url_hash(url):
    """Generate MD5 hash for a URL."""
    return hashlib.md5(url.encode()).hexdigest()


# NEW: Enhanced hash generation for structured data entries
def generate_structured_data_hash(base_url, table_index, row_index):
    """
    Generate a consistent hash for structured data entries.
    
    Args:
        base_url (str): The base URL of the page
        table_index (int): Index of the table on the page
        row_index (int): Index of the row in the table
        
    Returns:
        str: MD5 hash for the structured data entry
    """
    structured_identifier = f"{base_url}#table_{table_index}_row_{row_index}"
    return generate_url_hash(structured_identifier)


# NEW: Generate hash from multiple components for batch tracking
def generate_batch_hash(customer, project, timestamp_str):
    """
    Generate a hash for batch identification.
    
    Args:
        customer (str): Customer identifier
        project (str): Project identifier  
        timestamp_str (str): Timestamp string
        
    Returns:
        str: MD5 hash for the batch
    """
    batch_identifier = f"{customer}_{project}_{timestamp_str}"
    return hashlib.md5(batch_identifier.encode()).hexdigest()


# NEW: Sanitize identifiers for use in GCS paths and filenames
def sanitize_identifier(identifier, max_length=50):
    """
    Sanitize an identifier for safe use in filenames and paths.
    
    Args:
        identifier (str): The identifier to sanitize
        max_length (int): Maximum length of the sanitized identifier
        
    Returns:
        str: Sanitized identifier
    """
    # Remove or replace problematic characters
    sanitized = re.sub(r'[^\w\-_.]', '_', identifier)
    # Remove consecutive underscores
    sanitized = re.sub(r'_+', '_', sanitized)
    # Trim leading/trailing underscores and dots
    sanitized = sanitized.strip('_.')
    # Truncate if too long
    if len(sanitized) > max_length:
        sanitized = sanitized[:max_length].rstrip('_.')
    # Ensure not empty
    return sanitized if sanitized else 'unknown'


# NEW: Generate filename from URL for better document identification
def generate_filename_from_url(url, prefix="", suffix="", max_length=100):
    """
    Generate a readable filename from a URL.
    
    Args:
        url (str): The URL to convert
        prefix (str): Optional prefix for the filename
        suffix (str): Optional suffix for the filename
        max_length (int): Maximum length of the filename
        
    Returns:
        str: Generated filename
    """
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        
        # Extract meaningful parts from the URL
        path_parts = [part for part in parsed.path.split('/') if part]
        if path_parts:
            # Use the last meaningful path component
            base_name = path_parts[-1]
            # Remove file extension if present
            base_name = re.sub(r'\.[^.]*$', '', base_name)
        else:
            # Use domain if no path
            base_name = parsed.netloc.replace('.', '_')
        
        # Clean up the base name
        base_name = sanitize_identifier(base_name)
        
        # Construct final filename
        filename_parts = []
        if prefix:
            filename_parts.append(sanitize_identifier(prefix))
        filename_parts.append(base_name)
        if suffix:
            filename_parts.append(sanitize_identifier(suffix))
        
        filename = '_'.join(filename_parts)
        
        # Truncate if too long
        if len(filename) > max_length:
            filename = filename[:max_length].rstrip('_.')
        
        return filename if filename else 'document'
        
    except Exception:
        # Fallback to hash-based name
        return f"doc_{generate_url_hash(url)[:8]}"


# NEW: Extract date components for better organization
def extract_date_components(date_str=None):
    """
    Extract date components for organizing files and data.
    
    Args:
        date_str (str, optional): Date string in ISO format. If None, uses current date.
        
    Returns:
        dict: Dictionary with date components (year, month, day, date_str)
    """
    try:
        if date_str:
            if isinstance(date_str, str):
                # Try to parse ISO format
                dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            else:
                dt = date_str
        else:
            dt = datetime.now()
        
        return {
            'year': dt.year,
            'month': dt.month,
            'day': dt.day,
            'date_str': dt.strftime('%Y%m%d'),
            'month_str': dt.strftime('%Y%m'),
            'iso_date': dt.isoformat()
        }
    except Exception:
        # Fallback to current date
        dt = datetime.now()
        return {
            'year': dt.year,
            'month': dt.month,
            'day': dt.day,
            'date_str': dt.strftime('%Y%m%d'),
            'month_str': dt.strftime('%Y%m'),
            'iso_date': dt.isoformat()
        }


def setup_logging(customer_id, project_id, level=logging.WARNING):
    """Set up a Cloud Logging logger with customer and project context.

    Args:
        customer_id (str): The customer identifier.
        project_id (str): The project identifier.
        level (int, optional): The minimum logging level for this handler.
                               Defaults to logging.WARNING.
                               Use logging.INFO for more details, logging.DEBUG for extensive tracing.
    """
    # Get the root logger to configure its handlers
    root_logger = logging.getLogger()

    # Remove existing handlers to avoid duplicate logs if setup_logging is called multiple times
    for handler in root_logger.handlers[:]:
        if isinstance(handler, gcp_logging.handlers.CloudLoggingHandler):
            root_logger.removeHandler(handler)

    gcp_client = gcp_logging.Client()
    
    # NEW: Enhanced log name with better structure
    # Include timestamp to help with debugging pipeline runs
    log_timestamp = datetime.now().strftime('%Y%m%d_%H')
    cloud_log_name = f"{customer_id}_{project_id}_{log_timestamp}"
    
    handler = gcp_logging.handlers.CloudLoggingHandler(gcp_client, name=cloud_log_name)
    
    # Set the desired level ON THE HANDLER
    handler.setLevel(level)
    root_logger.addHandler(handler)
    root_logger.setLevel(level)

    # Get a specific logger for this context
    configured_logger = logging.getLogger(f"app.{customer_id}.{project_id}")
    
    # NEW: Add structured logging context
    # This helps correlate logs across different functions in the pipeline
    configured_logger = logging.LoggerAdapter(configured_logger, {
        'customer_id': customer_id,
        'project_id': project_id,
        'pipeline_run': log_timestamp
    })
    
    configured_logger.debug(f"Cloud Logging handler initialized for '{cloud_log_name}' with level {logging.getLevelName(level)}.")
    
    return configured_logger


# NEW: Helper for creating GCS paths with consistent structure
def build_gcs_path(template, **kwargs):
    """
    Build a GCS path from a template with proper formatting.
    
    Args:
        template (str): Path template with placeholders
        **kwargs: Values to substitute in the template
        
    Returns:
        str: Formatted GCS path
    """
    try:
        # Add date components if not provided
        if 'date_str' not in kwargs:
            date_components = extract_date_components()
            kwargs.update(date_components)
        
        # Sanitize string values
        sanitized_kwargs = {}
        for key, value in kwargs.items():
            if isinstance(value, str):
                sanitized_kwargs[key] = sanitize_identifier(value, max_length=100)
            else:
                sanitized_kwargs[key] = value
        
        # Format the template
        formatted_path = template.format(**sanitized_kwargs)
        
        # Clean up any double slashes
        formatted_path = re.sub(r'/+', '/', formatted_path)
        
        return formatted_path.strip('/')
        
    except Exception as e:
        # Fallback to basic path
        return f"unknown/{kwargs.get('date_str', datetime.now().strftime('%Y%m%d'))}"


# NEW: Validate and format URLs consistently
def normalize_url(url):
    """
    Normalize a URL for consistent processing.
    
    Args:
        url (str): The URL to normalize
        
    Returns:
        str: Normalized URL
    """
    if not url:
        return url
    
    # Strip whitespace
    url = url.strip()
    
    # Ensure proper protocol
    if url.startswith('//'):
        url = 'https:' + url
    elif not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    return url

def generate_csv_row_hash(csv_content, row_index, identifier_columns=None):
    """
    Generate a consistent hash for a CSV row to use as document ID.
    
    Args:
        csv_content (dict): Dictionary representing a CSV row
        row_index (int): Index of the row in the CSV
        identifier_columns (list): List of column names to use for ID generation
        
    Returns:
        str: MD5 hash for the CSV row
    """
    if identifier_columns:
        # Use specific columns for ID generation
        id_parts = []
        for col in identifier_columns:
            id_parts.append(str(csv_content.get(col, "")))
        identifier = "_".join(id_parts)
    else:
        # Use row index and a few key fields
        key_fields = ["OPERATION TYPE", "SECURITY TYPE AND MATURITY", "PERIOD", "DATE"]
        id_parts = [str(row_index)]
        for field in key_fields:
            if field in csv_content:
                id_parts.append(str(csv_content[field])[:20])  # Limit length
        identifier = "_".join(id_parts)
    
    return generate_url_hash(identifier)


def validate_csv_structure(csv_content, expected_columns, logger_instance):
    """
    Validate that a CSV has the expected column structure.
    
    Args:
        csv_content (str): Raw CSV content
        expected_columns (list): List of expected column names
        logger_instance: Logger instance
        
    Returns:
        tuple: (is_valid: bool, found_columns: list, missing_columns: list)
    """
    try:
        # Parse CSV to get columns
        df = pd.read_csv(StringIO(csv_content), nrows=0)  # Just get headers
        found_columns = df.columns.tolist()
        
        # Check for missing columns
        missing_columns = [col for col in expected_columns if col not in found_columns]
        
        is_valid = len(missing_columns) == 0
        
        if is_valid:
            logger_instance.info(f"CSV validation passed. Found all {len(expected_columns)} expected columns.")
        else:
            logger_instance.warning(f"CSV validation failed. Missing columns: {missing_columns}")
            logger_instance.debug(f"Found columns: {found_columns}")
        
        return is_valid, found_columns, missing_columns
        
    except Exception as e:
        logger_instance.error(f"Error validating CSV structure: {e}")
        return False, [], expected_columns


def parse_federal_reserve_date(date_string):
    """
    Parse various Federal Reserve date formats into a standardized format.
    
    Args:
        date_string (str): Date string from Federal Reserve sources
        
    Returns:
        str: Standardized date in YYYY-MM-DD format, or None if unparseable
    """
    if not date_string or not isinstance(date_string, str):
        return None
    
    import re
    from datetime import datetime
    
    # Remove extra whitespace
    date_string = date_string.strip()
    
    # Common Federal Reserve date patterns
    patterns = [
        (r'(\d{1,2})/(\d{1,2})/(\d{4})', '%m/%d/%Y'),  # MM/DD/YYYY or M/D/YYYY
        (r'(\d{4})-(\d{2})-(\d{2})', '%Y-%m-%d'),      # YYYY-MM-DD
        (r'(\w+)\s+(\d{1,2}),?\s+(\d{4})', '%B %d %Y'), # Month DD, YYYY
        (r'(\w+)\s+(\d{1,2})\s+(\d{4})', '%B %d %Y'),   # Month DD YYYY
    ]
    
    for pattern, format_str in patterns:
        try:
            match = re.search(pattern, date_string)
            if match:
                if format_str in ['%m/%d/%Y']:
                    # Handle MM/DD/YYYY format
                    month, day, year = match.groups()
                    dt = datetime(int(year), int(month), int(day))
                elif format_str == '%Y-%m-%d':
                    # Already in correct format
                    year, month, day = match.groups()
                    dt = datetime(int(year), int(month), int(day))
                else:
                    # Handle month name formats
                    dt = datetime.strptime(match.group(0), format_str)
                
                return dt.strftime('%Y-%m-%d')
                
        except (ValueError, AttributeError):
            continue
    
    return None


def extract_monetary_amount(amount_string):
    """
    Extract monetary amounts from Federal Reserve text.
    
    Args:
        amount_string (str): String containing monetary amount
        
    Returns:
        dict: Dictionary with currency, amount, and multiplier
    """
    if not amount_string or not isinstance(amount_string, str):
        return {"currency": "", "amount": "", "multiplier": ""}
    
    import re
    
    # Pattern to match currency, amount, and multiplier
    pattern = r'([\$€£¥]?)?\s*([\d,]+(?:\.\d+)?)\s*([a-zA-Z]+)?'
    match = re.search(pattern, amount_string.strip())
    
    if match:
        currency = match.group(1) or ""
        amount = match.group(2).replace(',', '') if match.group(2) else ""
        multiplier = match.group(3) or ""
        
        # Standardize multiplier
        if multiplier.lower().startswith('million'):
            multiplier = 'million'
        elif multiplier.lower().startswith('billion'):
            multiplier = 'billion'
        elif multiplier.lower().startswith('trillion'):
            multiplier = 'trillion'
        
        return {
            "currency": currency,
            "amount": amount,
            "multiplier": multiplier
        }
    
    return {"currency": "", "amount": "", "multiplier": ""}


def build_fedao_gcs_path(bucket_name, path_template, **kwargs):
    """
    Build GCS paths specifically for FEDAO data organization.
    
    Args:
        bucket_name (str): GCS bucket name
        path_template (str): Path template with placeholders
        **kwargs: Values for template substitution
        
    Returns:
        str: Complete GCS path
    """
    # Ensure we have date info
    if 'date_str' not in kwargs:
        from datetime import datetime
        kwargs['date_str'] = datetime.now().strftime('%Y%m%d')
    
    if 'batch_id' not in kwargs:
        from datetime import datetime
        kwargs['batch_id'] = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Build the path
    formatted_path = build_gcs_path(path_template, **kwargs)
    
    return f"gs://{bucket_name}/{formatted_path}"


def log_fedao_processing_stats(logger_instance, operation, **stats):
    """
    Log FEDAO processing statistics in a structured way.
    
    Args:
        logger_instance: Logger instance
        operation (str): Operation being performed
        **stats: Statistics to log
    """
    stats_str = ", ".join([f"{k}={v}" for k, v in stats.items()])
    logger_instance.info(f"FEDAO {operation} - {stats_str}")


# Add this to your existing setup_logging function to include FEDAO context
def setup_fedao_logging(customer_id, project_id, operation_type="", level=logging.INFO):
    """
    Set up logging specifically for FEDAO operations.
    
    Args:
        customer_id (str): Customer identifier
        project_id (str): Project identifier  
        operation_type (str): Type of FEDAO operation (scrape, transform, extract)
        level: Logging level
        
    Returns:
        Logger instance configured for FEDAO
    """
    # Use the existing setup_logging function
    logger = setup_logging(customer_id, project_id, level)
    
    # Add FEDAO-specific context
    fedao_logger = logging.LoggerAdapter(logger, {
        'fedao_operation': operation_type,
        'pipeline_type': 'FEDAO'
    })
    
    return fedao_logger