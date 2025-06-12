import json
import base64
import logging
import os
import requests
import pandas as pd
from io import StringIO, BytesIO
from datetime import datetime, timezone
import re
from urllib.parse import urljoin, urlparse
import functions_framework
from google.cloud import storage, pubsub_v1
from bs4 import BeautifulSoup
import tempfile
import traceback
from typing import List, Dict, Any, Optional

from src.common.utils import setup_logging
from src.common.config import load_customer_config, load_dynamic_site_config

# Initialize logger at the module level
logger = logging.getLogger(__name__)


class SmartFEDAOExtractor:
    """Enhanced FEDAO extractor with intelligent field detection"""
    
    def __init__(self, api_key: str = "AIzaSyDDLEY8JIrgPgiqEUHqyJh0He4xmIrwxJs"):
        self.api_key = api_key
        self.gemini_url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash-exp:generateContent"
        self.logger = logging.getLogger(__name__)

    def call_gemini_api(self, prompt: str, data: str) -> Optional[Dict]:
        """Call Gemini API with better error handling"""
        try:
            headers = {"Content-Type": "application/json"}
            
            payload = {
                "contents": [{
                    "parts": [{
                        "text": f"{prompt}\n\nHTML/Data to process:\n{data}"
                    }]
                }],
                "generationConfig": {
                    "temperature": 0.1,
                    "maxOutputTokens": 8192
                }
            }
            
            response = requests.post(
                f"{self.gemini_url}?key={self.api_key}",
                headers=headers,
                json=payload,
                timeout=60  # Increased timeout
            )
            
            if response.status_code == 200:
                result = response.json()
                if 'candidates' in result and len(result['candidates']) > 0:
                    content = result['candidates'][0]['parts'][0]['text']
                    try:
                        return json.loads(content)
                    except json.JSONDecodeError:
                        # Try to extract JSON from text
                        json_match = re.search(r'\[.*\]', content, re.DOTALL)
                        if json_match:
                            return json.loads(json_match.group(0))
                        return {"text": content}
                        
        except Exception as e:
            self.logger.error(f"Gemini API call failed: {e}")
            # Re-raise the exception to halt execution
        raise IOError(f"Gemini API call failed: {e}") from e

    def smart_html_preprocessing(self, html_content: str) -> str:
        """Intelligently preprocess HTML to preserve important data"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find all tables and their surrounding context
        important_sections = []
        
        # Look for operation-related tables
        tables = soup.find_all('table')
        for table in tables:
            # Get table with some context
            table_html = str(table)
            
            # Also get preceding headings/text that might contain metadata
            prev_elements = []
            current = table.previous_sibling
            context_chars = 0
            
            while current and context_chars < 1000:
                if hasattr(current, 'get_text'):
                    text = current.get_text(strip=True)
                    if text and ('operation' in text.lower() or 'schedule' in text.lower() or 'treasury' in text.lower()):
                        prev_elements.append(str(current))
                        context_chars += len(str(current))
                current = current.previous_sibling
            
            section = '\n'.join(reversed(prev_elements)) + '\n' + table_html
            important_sections.append(section)
        
        # Combine sections, prioritizing by likely relevance
        combined_html = '\n\n'.join(important_sections)
        
        # If still too long, prioritize tables with more data
        if len(combined_html) > 25000:  # Increased limit
            # Sort by table size and take the largest ones
            sections_with_size = [(len(section), section) for section in important_sections]
            sections_with_size.sort(reverse=True)
            
            combined_html = ""
            for size, section in sections_with_size:
                if len(combined_html) + size < 25000:
                    combined_html += section + "\n\n"
                else:
                    break
        
        return combined_html

    def extract_with_ai_enhanced(self, html_content: str, table_type: str) -> List[Dict]:
        """Enhanced AI extraction with better field detection"""
        
        # Preprocess HTML intelligently
        processed_html = self.smart_html_preprocessing(html_content)
        
        # Define flexible schemas
        if table_type == "MOA":
            expected_fields = {
                "OPERATION DATE": ["operation date", "date", "period", "effective date"],
                "OPERATION TIME(ET)": ["operation time", "time", "time (et)", "execution time"],
                "SETTLEMENT DATE": ["settlement date", "settle date", "settlement"],
                "OPERATION TYPE": ["operation type", "type", "operation", "action"],
                "SECURITY TYPE AND MATURITY": ["security type", "security", "maturity", "instrument"],
                "MATURITY RANGE": ["maturity range", "range", "duration", "term"],
                "MAXIMUM OPERATION SIZE": ["maximum operation size", "size", "amount", "volume", "maximum"]
            }
        else:  # TOA
            expected_fields = {
                "DATE": ["date", "operation date", "effective date"],
                "OPERATION TYPE": ["operation type", "type", "operation"],
                "SECURITY TYPE AND MATURITY": ["security type", "security", "maturity", "instrument"],
                "CUSIP": ["cusip", "security id", "identifier"],
                "MAXIMUM PURCHASE AMOUNT": ["maximum purchase", "purchase amount", "amount", "maximum"]
            }
        
        prompt = f"""
        You are an expert at extracting structured data from Federal Reserve HTML tables.
        
        Task: Extract {table_type} data from the HTML below, being flexible with column names.
        
        Expected Fields and Possible Variations:
        {json.dumps(expected_fields, indent=2)}
        
        CRITICAL Instructions:
        1. Find ALL tables in the HTML content
        2. For each table, examine headers carefully - they may have slight variations
        3. Map headers to expected fields using semantic similarity (not exact matching)
        4. Extract ALL data rows, even if some fields are missing
        5. For date ranges (e.g., "01/13/2025 - 01/17/2025"), preserve the full range but also extract the end date
        6. For operation amounts, keep the full text including currency and multipliers
        7. If operation time is in a separate column or cell, extract it
        8. Look for settlement dates even if in different columns
        9. Extract maturity information even if it's combined with security type
        10. Split multi-line cells into separate rows where appropriate
        
        Special Handling:
        - If "PERIOD" contains date ranges, extract both the range and the end date
        - If time information appears anywhere in the row, capture it
        - If settlement information is available, extract it
        - Look for maturity ranges or terms in any column
        
        Output Format:
        Return a JSON array where each object represents one operation with all available fields:
        [
          {{
            "OPERATION DATE": "end date from range or single date",
            "OPERATION TIME(ET)": "time if available, empty string if not",
            "SETTLEMENT DATE": "settlement date if available, empty string if not", 
            "OPERATION TYPE": "operation type",
            "SECURITY TYPE AND MATURITY": "security and maturity info",
            "MATURITY RANGE": "maturity range if specified, empty string if not",
            "MAXIMUM OPERATION SIZE": "full amount text with currency"
          }}
        ]
        
        Return ONLY the JSON array, no other text.
        """
        
        result = self.call_gemini_api(prompt, processed_html)
        
        if result and isinstance(result, list):
            self.logger.info(f"AI extracted {len(result)} {table_type} operations")
            return result
        elif result and result.get('text'):
            # Try to extract JSON from text response
            try:
                json_match = re.search(r'\[.*\]', result['text'], re.DOTALL)
                if json_match:
                    return json.loads(json_match.group(0))
            except json.JSONDecodeError:
                pass
        
        self.logger.warning(f"AI extraction failed for {table_type}, using enhanced fallback")
        return self._enhanced_fallback_extraction(html_content, table_type)

    def _enhanced_fallback_extraction(self, html_content: str, table_type: str) -> List[Dict]:
        """Enhanced fallback extraction with better field detection"""
        data = []
        soup = BeautifulSoup(html_content, 'html.parser')
        
        for table in soup.find_all('table'):
            rows = table.find_all('tr')
            if len(rows) < 2:
                continue
                
            # Get headers and normalize them
            header_row = rows[0]
            headers = []
            for th in header_row.find_all(['th', 'td']):
                header_text = th.get_text(strip=True).upper()
                # Normalize common variations
                header_text = re.sub(r'\s+', ' ', header_text)
                headers.append(header_text)
            
            self.logger.info(f"Table headers found: {headers}")
            
            # Create mapping from normalized headers to expected fields
            field_mapping = self._create_flexible_mapping(headers, table_type)
            self.logger.info(f"Field mapping: {field_mapping}")
            
            # Process data rows
            for row in rows[1:]:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:  # Minimum cells for meaningful data
                    row_data = {}
                    
                    # Initialize all expected fields
                    if table_type == "MOA":
                        expected_keys = ["OPERATION DATE", "OPERATION TIME(ET)", "SETTLEMENT DATE", 
                                       "OPERATION TYPE", "SECURITY TYPE AND MATURITY", "MATURITY RANGE", 
                                       "MAXIMUM OPERATION SIZE"]
                    else:
                        expected_keys = ["DATE", "OPERATION TYPE", "SECURITY TYPE AND MATURITY", 
                                       "CUSIP", "MAXIMUM PURCHASE AMOUNT"]
                    
                    for key in expected_keys:
                        row_data[key] = ""
                    
                    # Extract data based on mapping
                    for i, cell in enumerate(cells):
                        if i < len(headers):
                            cell_text = cell.get_text(separator=' ', strip=True)
                            header = headers[i]
                            
                            # Map to expected field
                            mapped_field = field_mapping.get(header)
                            if mapped_field:
                                row_data[mapped_field] = cell_text
                            
                            # Special handling for period/date ranges
                            if 'PERIOD' in header and table_type == "MOA":
                                if ' - ' in cell_text:
                                    parts = cell_text.split(' - ')
                                    if len(parts) == 2:
                                        row_data["OPERATION DATE"] = parts[1].strip()  # End date
                                else:
                                    row_data["OPERATION DATE"] = cell_text
                    
                    # Only add row if it has meaningful data
                    if any(value.strip() for value in row_data.values()):
                        data.append(row_data)
        
        return data

    def _create_flexible_mapping(self, headers: List[str], table_type: str) -> Dict[str, str]:
        """Create flexible mapping from source headers to expected fields"""
        mapping = {}
        
        if table_type == "MOA":
            field_patterns = {
                "OPERATION DATE": [r".*operation.*date.*", r".*date.*", r".*period.*"],
                "OPERATION TIME(ET)": [r".*time.*", r".*execution.*time.*"],
                "SETTLEMENT DATE": [r".*settlement.*date.*", r".*settle.*"],
                "OPERATION TYPE": [r".*operation.*type.*", r".*type.*", r".*operation(?!.*date).*"],
                "SECURITY TYPE AND MATURITY": [r".*security.*", r".*maturity.*", r".*instrument.*"],
                "MATURITY RANGE": [r".*maturity.*range.*", r".*range.*", r".*duration.*"],
                "MAXIMUM OPERATION SIZE": [r".*maximum.*", r".*size.*", r".*amount.*", r".*volume.*"]
            }
        else:  # TOA
            field_patterns = {
                "DATE": [r".*date.*"],
                "OPERATION TYPE": [r".*operation.*type.*", r".*type.*"],
                "SECURITY TYPE AND MATURITY": [r".*security.*", r".*maturity.*"],
                "CUSIP": [r".*cusip.*", r".*security.*id.*"],
                "MAXIMUM PURCHASE AMOUNT": [r".*maximum.*purchase.*", r".*purchase.*amount.*", r".*amount.*"]
            }
        
        for header in headers:
            header_lower = header.lower()
            for expected_field, patterns in field_patterns.items():
                for pattern in patterns:
                    if re.search(pattern, header_lower):
                        if expected_field not in mapping.values():  # Avoid duplicates
                            mapping[header] = expected_field
                            break
                if header in mapping:
                    break
        
        return mapping

    def post_process_extracted_data(self, data: List[Dict], table_type: str) -> List[Dict]:
        """Post-process extracted data to fill missing fields intelligently"""
        processed_data = []
        
        for row in data:
            processed_row = row.copy()
            
            # Clean and standardize operation date
            if table_type == "MOA" and processed_row.get("OPERATION DATE"):
                date_str = processed_row["OPERATION DATE"]
                if ' - ' in date_str:
                    parts = date_str.split(' - ')
                    if len(parts) == 2:
                        processed_row["OPERATION DATE"] = parts[1].strip()
            
            # Try to extract time information from other fields if missing
            if table_type == "MOA" and not processed_row.get("OPERATION TIME(ET)"):
                # Look for time patterns in other fields
                for field_name, field_value in processed_row.items():
                    if field_value and isinstance(field_value, str):
                        time_match = re.search(r'(\d{1,2}:\d{2}(?:\s*[AP]M)?)', field_value, re.IGNORECASE)
                        if time_match:
                            processed_row["OPERATION TIME(ET)"] = time_match.group(1)
                            break
            
            # Ensure all required fields exist
            if table_type == "MOA":
                required_fields = ["OPERATION DATE", "OPERATION TIME(ET)", "SETTLEMENT DATE", 
                                 "OPERATION TYPE", "SECURITY TYPE AND MATURITY", "MATURITY RANGE", 
                                 "MAXIMUM OPERATION SIZE"]
            else:
                required_fields = ["DATE", "OPERATION TYPE", "SECURITY TYPE AND MATURITY", 
                                 "CUSIP", "MAXIMUM PURCHASE AMOUNT"]
            
            for field in required_fields:
                if field not in processed_row:
                    processed_row[field] = ""
            
            processed_data.append(processed_row)
        
        return processed_data

# Enhanced extraction functions
def scrape_treasury_operations_enhanced_ai(treasury_url: str, extractor: SmartFEDAOExtractor) -> List[Dict]:
    """Enhanced Treasury operations scraping with smart field detection"""
    extractor.logger.info(f"Enhanced AI scraping Treasury operations from: {treasury_url}")
    
    try:
        response = requests.get(treasury_url, timeout=30)
        response.raise_for_status()
        
        # Extract MOA data using enhanced AI
        moa_data = extractor.extract_with_ai_enhanced(response.text, "MOA")
        
        # Post-process to fill missing fields
        standardized_moa = extractor.post_process_extracted_data(moa_data, "MOA")
        
        extractor.logger.info(f"Enhanced AI extracted {len(standardized_moa)} MOA operations")
        return standardized_moa
        
    except Exception as e:
        extractor.logger.error(f"Error in enhanced AI Treasury scraping: {e}")
        return []

def scrape_ambs_operations_enhanced_ai(ambs_url: str, extractor: SmartFEDAOExtractor) -> List[Dict]:
    """Enhanced AMBS operations scraping with smart field detection"""
    extractor.logger.info(f"Enhanced AI scraping AMBS operations from: {ambs_url}")
    
    try:
        response = requests.get(ambs_url, timeout=30)
        response.raise_for_status()
        
        # Extract TOA data using enhanced AI
        toa_data = extractor.extract_with_ai_enhanced(response.text, "TOA")
        
        # Post-process to fill missing fields
        standardized_toa = extractor.post_process_extracted_data(toa_data, "TOA")
        
        extractor.logger.info(f"Enhanced AI extracted {len(standardized_toa)} TOA operations")
        return standardized_toa
        
    except Exception as e:
        extractor.logger.error(f"Error in enhanced AI AMBS scraping: {e}")
        return []

# Keep the original functions as fallbacks
def extract_treasury_operations_table(soup, base_url):
    """Extract data from the main Treasury operations table - FIXED VERSION"""
    operations_data = []
    active_logger = logging.getLogger(__name__)
    
    table = soup.find('table', {'id': 'pagination-table'})
    if not table:
        active_logger.warning("Could not find main Treasury operations table (id='pagination-table')")
        return operations_data
    
    tbody = table.find('tbody')
    if not tbody:
        active_logger.warning("Could not find tbody in main Treasury operations table")
        return operations_data
        
    rows = tbody.find_all('tr')
    active_logger.info(f"Found {len(rows)} rows in main Treasury operations table.")
    
    for i, row in enumerate(rows):
        cells = row.find_all('td')
        if len(cells) >= 4:
            # FIXED: Use proper column mapping to avoid duplicate dates
            period = cells[0].get_text(strip=True).replace('<br>', ' - ')
            planned_amount_text = cells[1].get_text(strip=True)
            
            # Parse the period to extract the operation date (end date of range)
            operation_date = ""
            if ' - ' in period:
                date_parts = period.split(' - ')
                if len(date_parts) == 2:
                    operation_date = date_parts[1].strip()  # Use end date
            else:
                operation_date = period
            
            operation_type_val = "No Operations"
            max_operation_size_val = ""
            security_type_val = "N/A"

            # Enhanced parsing logic
            if "small value purchase" in planned_amount_text.lower():
                operation_type_val = "Small Value Purchase"
                security_type_val = "Treasury Securities"
            elif "purchase operation" in planned_amount_text.lower():
                operation_type_val = "Purchase Operation"
                if "treasury" in planned_amount_text.lower():
                     security_type_val = "Treasury Securities"
            elif "repo operation" in planned_amount_text.lower():
                operation_type_val = "Repo Operation"
            elif "reverse repo operation" in planned_amount_text.lower():
                operation_type_val = "Reverse Repo Operation"

            amount_match = re.search(r'\$[\d,]+(?:\.\d+)?\s*(million|billion)?', planned_amount_text, re.IGNORECASE)
            if amount_match:
                max_operation_size_val = amount_match.group(0)
            else:
                if "as appropriate" in planned_amount_text.lower() or "variable amount" in planned_amount_text.lower():
                    max_operation_size_val = planned_amount_text
                else:
                    max_operation_size_val = ""

            # FIXED: Use standard column names and don't include URLs
            operations_data.append({
                'OPERATION DATE': operation_date,  # Single date, not period
                'OPERATION TYPE': operation_type_val,
                'SECURITY TYPE AND MATURITY': security_type_val,
                'MAXIMUM OPERATION SIZE': max_operation_size_val,
            })
        else:
            active_logger.warning(f"Skipping row {i+1} in main operations table, not enough cells ({len(cells)} found).")
    
    active_logger.info(f"Extracted {len(operations_data)} items from main Treasury operations table.")
    return operations_data

def extract_current_schedule_table(soup):
    """Extract data from the Current Schedule tab - FIXED VERSION"""
    schedule_data = []
    active_logger = logging.getLogger(__name__)

    current_schedule_div = soup.find('div', {'id': 'current-schedule'})
    if not current_schedule_div:
        active_logger.info("Current schedule div (id='current-schedule') not found.")
        return schedule_data
    
    table = current_schedule_div.find('table')
    if not table:
        active_logger.info("Table not found within current schedule div.")
        return schedule_data
    
    tbody = table.find('tbody')
    if not tbody or not tbody.find_all('tr', recursive=False):
        active_logger.info("Current schedule table tbody is empty or has no direct tr children.")
        return schedule_data
    
    rows = tbody.find_all('tr', recursive=False)
    active_logger.info(f"Found {len(rows)} rows in current schedule table.")
    
    for i, row in enumerate(rows):
        cells = row.find_all('td')
        if len(cells) >= 7:
            schedule_data.append({
                'OPERATION DATE': cells[0].get_text(strip=True),
                'OPERATION TIME(ET)': cells[1].get_text(strip=True),
                'SETTLEMENT DATE': cells[2].get_text(strip=True),
                'OPERATION TYPE': cells[3].get_text(strip=True),
                'SECURITY TYPE AND MATURITY': cells[4].get_text(strip=True),
                'MATURITY RANGE': cells[5].get_text(strip=True),
                'MAXIMUM OPERATION SIZE': cells[6].get_text(strip=True)
            })
        else:
             active_logger.warning(f"Skipping row {i+1} in current schedule, not enough cells ({len(cells)} found, expected >=7).")
    
    active_logger.info(f"Extracted {len(schedule_data)} items from current schedule table.")
    return schedule_data

def scrape_ambs_operations(ambs_url, active_logger):
    """Scrape AMBS operations from the second URL"""
    ambs_data = []
    try:
        active_logger.info(f"Scraping AMBS operations from: {ambs_url}")
        response = requests.get(ambs_url, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        all_tables = soup.find_all('table')
        if not all_tables:
            active_logger.warning(f"No tables found on AMBS page: {ambs_url}")
            return ambs_data
        
        active_logger.info(f"Found {len(all_tables)} tables on AMBS page. Inspecting for operations data...")

        for table_idx, table in enumerate(all_tables):
            thead = table.find('thead')
            tbody = table.find('tbody')

            if not (thead and tbody):
                active_logger.debug(f"Skipping table {table_idx+1} on AMBS page: missing thead or tbody.")
                continue

            header_texts_th = [th.get_text(strip=True).lower() for th in thead.find_all('th')]
            header_text_concat = " ".join(header_texts_th)

            if not ('operation' in header_text_concat and \
                    ('date' in header_text_concat or 'cusip' in header_text_concat or 'security' in header_text_concat)):
                active_logger.debug(f"Skipping table {table_idx+1} on AMBS page: headers {header_texts_th} don't seem to match AMBS operations.")
                continue
            
            active_logger.info(f"Processing potentially relevant AMBS table {table_idx+1} with headers: {header_texts_th}")
            
            actual_headers = [th.get_text(strip=True) for th in thead.find_all('th') if th.get_text(strip=True)]

            rows = tbody.find_all('tr')
            for row_idx, row in enumerate(rows):
                cells = row.find_all('td')
                if len(cells) == len(actual_headers):
                    row_data = {}
                    for i, cell in enumerate(cells):
                        cell_text = cell.get_text(separator='\n', strip=True)
                        row_data[actual_headers[i]] = cell_text
                    
                    if 'Operation Date' in row_data:
                        row_data['DATE'] = row_data['Operation Date'] 
                    elif 'Date' in row_data and 'DATE' not in row_data:
                         row_data['DATE'] = row_data['Date']

                    ambs_data.append(row_data)
                elif cells:
                    active_logger.warning(f"Skipping row {row_idx+1} in AMBS table {table_idx+1}: cell count ({len(cells)}) != header count ({len(actual_headers)}).")
        
        active_logger.info(f"Extracted {len(ambs_data)} AMBS operations rows.")
        
    except requests.exceptions.RequestException as e_req:
        active_logger.error(f"Request error scraping AMBS operations {ambs_url}: {str(e_req)}")
    except Exception as e:
        active_logger.error(f"General error scraping AMBS operations {ambs_url}: {str(e)}", exc_info=True)
    
    return ambs_data

def upload_csv_to_gcs(csv_content: str, bucket_name: str, file_path: str, active_logger) -> str:
    """Upload CSV content to GCS"""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        
        blob.upload_from_string(csv_content, content_type='text/csv')
        active_logger.info(f"Successfully uploaded CSV to gs://{bucket_name}/{file_path}")
        return f"gs://{bucket_name}/{file_path}"
        
    except Exception as e:
        active_logger.error(f"Error uploading CSV to GCS gs://{bucket_name}/{file_path}: {str(e)}")
        raise

@functions_framework.cloud_event
def scrape_fedao_sources_ai(cloud_event):
    """AI-powered FEDAO source scraping and CSV generation"""
    
    # Initialize logger for the function call
    active_logger = setup_logging("simba", "fedao_project")
    active_logger.info("AI-powered Cloud Function 'scrape_fedao_sources_ai' triggered.")
    
    try:
        # Load configuration
        customer_config = load_customer_config("simba")
        gcp_project_id = customer_config.get("gcp_project_id", os.environ.get("GCP_PROJECT"))
        active_logger.info(f"Using GCP Project ID: {gcp_project_id}")
        
        # Initialize AI extractor
        extractor = AITableExtractor()
        
        treasury_url = os.environ.get("FEDAO_TREASURY_URL", 
            "https://www.newyorkfed.org/markets/domestic-market-operations/monetary-policy-implementation/treasury-securities/treasury-securities-operational-details")
        ambs_url = os.environ.get("FEDAO_AMBS_URL",
            "https://www.newyorkfed.org/markets/ambs_operation_schedule#tabs-2")
        
        output_bucket = os.environ.get("FEDAO_OUTPUT_BUCKET", "execo-simba-fedao-poc")
        active_logger.info(f"Output GCS bucket: {output_bucket}")

        # Try AI-powered extraction first, fallback to original if needed
        ai_enabled = os.environ.get("AI_PROCESSING_ENABLED", "true").lower() == "true"
        
        if ai_enabled:
            # AI-powered Treasury Operations scraping (for MOA)
            moa_data = scrape_treasury_operations_ai(treasury_url, extractor)
            
            if not moa_data:
                active_logger.warning("AI extraction failed for MOA, trying fallback method")
                # Fallback to original method
                response_treasury = requests.get(treasury_url, timeout=30)
                response_treasury.raise_for_status()
                soup_treasury = BeautifulSoup(response_treasury.content, 'html.parser')
                
                treasury_ops_main_table = extract_treasury_operations_table(soup_treasury, treasury_url)
                treasury_ops_current_schedule = extract_current_schedule_table(soup_treasury)
                moa_data = treasury_ops_main_table + treasury_ops_current_schedule
        else:
            # Use original method
            response_treasury = requests.get(treasury_url, timeout=30)
            response_treasury.raise_for_status()
            soup_treasury = BeautifulSoup(response_treasury.content, 'html.parser')
            
            treasury_ops_main_table = extract_treasury_operations_table(soup_treasury, treasury_url)
            treasury_ops_current_schedule = extract_current_schedule_table(soup_treasury)
            moa_data = treasury_ops_main_table + treasury_ops_current_schedule
        
        moa_raw_file_path = f"FEDAO/inputs/raw_manual_uploads/FEDAO_MOA_RAW_DATA.csv"
        moa_file_url = ""

        if moa_data:
            moa_df = pd.DataFrame(moa_data)
            active_logger.info(f"{'AI' if ai_enabled else 'Standard'} collected {len(moa_df)} rows for MOA.")
            active_logger.info(f"MOA DataFrame columns: {moa_df.columns.tolist()}")
            
            # Add source date
            current_date = datetime.now().strftime('%Y%m%d')
            moa_df['Source_Date'] = current_date
            
            moa_csv_content = moa_df.to_csv(index=False)
            moa_file_url = upload_csv_to_gcs(moa_csv_content, output_bucket, moa_raw_file_path, active_logger)
            active_logger.info(f"Generated MOA RAW CSV with {len(moa_df)} rows.")
        else:
            active_logger.info("No MOA data collected. MOA RAW CSV will not be generated.")
        
        # Handle TOA data similarly
        if ai_enabled:
            # AI-powered AMBS Operations scraping (for TOA)
            toa_data = scrape_ambs_operations_ai(ambs_url, extractor)
            
            if not toa_data:
                active_logger.warning("AI extraction failed for TOA, trying fallback method")
                toa_data = scrape_ambs_operations(ambs_url, active_logger)
        else:
            toa_data = scrape_ambs_operations(ambs_url, active_logger)
        
        toa_raw_file_path = f"FEDAO/inputs/raw_manual_uploads/FEDAO_TOA_RAW_DATA.csv"
        toa_file_url = ""

        if toa_data:
            toa_df = pd.DataFrame(toa_data)
            active_logger.info(f"{'AI' if ai_enabled else 'Standard'} collected {len(toa_df)} rows for TOA.")
            active_logger.info(f"TOA DataFrame columns: {toa_df.columns.tolist()}")
            
            # Add source date
            current_date = datetime.now().strftime('%Y%m%d')
            toa_df['Source_Date'] = current_date
            
            toa_csv_content = toa_df.to_csv(index=False)
            toa_file_url = upload_csv_to_gcs(toa_csv_content, output_bucket, toa_raw_file_path, active_logger)
            active_logger.info(f"Generated TOA RAW CSV with {len(toa_df)} rows.")
        else:
            active_logger.info("No TOA data collected. TOA RAW CSV will not be generated.")
        
        active_logger.info(f"{'AI-powered' if ai_enabled else 'Standard'} FEDAO source scraping completed successfully.")
        return {
            "status": "success",
            "method": f"{'AI-powered' if ai_enabled else 'Standard'} extraction",
            "moa_rows_collected": len(moa_data) if moa_data else 0,
            "toa_rows_collected": len(toa_data) if toa_data else 0,
            "moa_raw_file": moa_file_url if moa_file_url else "No MOA file generated",
            "toa_raw_file": toa_file_url if toa_file_url else "No TOA file generated"
        }
        
    except Exception as e:
        active_logger.error(f"Critical error in 'scrape_fedao_sources_ai' function: {str(e)}", exc_info=True)
        return {
            "status": "error", 
            "message": str(e),
            "method": f"{'AI-powered' if ai_enabled else 'Standard'} extraction",
            "moa_rows_collected": 0,
            "toa_rows_collected": 0,
            "moa_raw_file": "Error occurred",
            "toa_raw_file": "Error occurred"
        }

# Keep the original function for backward compatibility
@functions_framework.cloud_event
def scrape_fedao_sources(cloud_event):
    """Original FEDAO source scraping function - kept for fallback"""
    
    # Initialize logger for the function call
    active_logger = setup_logging("simba", "fedao_project")
    active_logger.info("Cloud Function 'scrape_fedao_sources' triggered.")
    
    try:
        # Load configuration
        customer_config = load_customer_config("simba")
        gcp_project_id = customer_config.get("gcp_project_id", os.environ.get("GCP_PROJECT"))
        active_logger.info(f"Using GCP Project ID: {gcp_project_id}")
        
        treasury_url = os.environ.get("FEDAO_TREASURY_URL", 
            "https://www.newyorkfed.org/markets/domestic-market-operations/monetary-policy-implementation/treasury-securities/treasury-securities-operational-details")
        ambs_url = os.environ.get("FEDAO_AMBS_URL",
            "https://www.newyorkfed.org/markets/ambs_operation_schedule#tabs-2")
        
        output_bucket = os.environ.get("FEDAO_OUTPUT_BUCKET", "execo-simba-fedao-poc")
        active_logger.info(f"Output GCS bucket: {output_bucket}")

        # Scrape Treasury Operations (for MOA) - HTML tables only
        active_logger.info(f"Scraping Treasury operations from: {treasury_url}")
        response_treasury = requests.get(treasury_url, timeout=30)
        response_treasury.raise_for_status()
        soup_treasury = BeautifulSoup(response_treasury.content, 'html.parser')
        
        treasury_ops_main_table = extract_treasury_operations_table(soup_treasury, treasury_url)
        treasury_ops_current_schedule = extract_current_schedule_table(soup_treasury)
        
        all_moa_data_list = []
        all_moa_data_list.extend(treasury_ops_main_table)
        all_moa_data_list.extend(treasury_ops_current_schedule)
        
        moa_raw_file_path = f"FEDAO/inputs/raw_manual_uploads/FEDAO_MOA_RAW_DATA.csv"
        moa_file_url = ""

        if all_moa_data_list:
            moa_df = pd.DataFrame(all_moa_data_list)
            active_logger.info(f"Collected {len(moa_df)} raw rows for MOA from HTML sources only.")
            active_logger.info(f"MOA Raw DataFrame columns before CSV conversion: {moa_df.columns.tolist()}")
            
            moa_csv_content = moa_df.to_csv(index=False) 
            
            moa_file_url = upload_csv_to_gcs(moa_csv_content, output_bucket, moa_raw_file_path, active_logger)
            active_logger.info(f"Generated MOA RAW CSV with {len(moa_df)} rows.")
        else:
            active_logger.info("No data collected for MOA. MOA RAW CSV will not be generated.")
        
        # Scrape AMBS Operations (for TOA)
        toa_ambs_data = scrape_ambs_operations(ambs_url, active_logger)
        
        toa_raw_file_path = f"FEDAO/inputs/raw_manual_uploads/FEDAO_TOA_RAW_DATA.csv"
        toa_file_url = ""

        if toa_ambs_data:
            toa_df = pd.DataFrame(toa_ambs_data)
            active_logger.info(f"Collected {len(toa_df)} raw rows for TOA.")
            active_logger.info(f"TOA Raw DataFrame columns before CSV conversion: {toa_df.columns.tolist()}")
            
            toa_csv_content = toa_df.to_csv(index=False)
            
            toa_file_url = upload_csv_to_gcs(toa_csv_content, output_bucket, toa_raw_file_path, active_logger)
            active_logger.info(f"Generated TOA RAW CSV with {len(toa_df)} rows.")
        else:
            active_logger.info("No data collected for TOA. TOA RAW CSV will not be generated.")
        
        active_logger.info("FEDAO source scraping completed.")
        return {
            "status": "success",
            "moa_rows_collected": len(all_moa_data_list) if all_moa_data_list else 0,
            "toa_rows_collected": len(toa_ambs_data) if toa_ambs_data else 0,
            "moa_raw_file": moa_file_url if moa_file_url else "No MOA file generated",
            "toa_raw_file": toa_file_url if toa_file_url else "No TOA file generated"
        }
        
    except Exception as e:
        active_logger.error(f"Critical error in 'scrape_fedao_sources' function: {str(e)}", exc_info=True)
        return {
            "status": "error", 
            "message": str(e),
            "moa_rows_collected": 0,
            "toa_rows_collected": 0,
            "moa_raw_file": "Error occurred",
            "toa_raw_file": "Error occurred"
        }