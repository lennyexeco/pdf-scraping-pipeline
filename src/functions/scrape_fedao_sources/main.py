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
from google.cloud import aiplatform
from vertexai.generative_models import GenerativeModel, Part
import pdfplumber
from pathlib import Path
import csv

# Initialize logger at the module level
logger = logging.getLogger(__name__)

# INLINE UTILITY FUNCTIONS
def setup_logging(customer_id: str, project_name: str):
    """Setup minimal logging configuration."""
    logging.basicConfig(
        level=logging.WARNING,  # Changed from INFO to WARNING
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(f"{customer_id}.{project_name}")

# INLINE CONFIG FUNCTIONS
def load_customer_config(customer_id: str):
    """Load customer configuration."""
    return {
        "gcp_project_id": os.environ.get("GCP_PROJECT", "execo-simba")
    }

def load_dynamic_site_config(db, project_name: str, logger):
    """Load dynamic site configuration."""
    return {}

class DataValidationError(Exception):
    """Custom exception for data validation failures"""
    pass

class ExtractionValidator:
    """Simplified validation system for extracted data"""
    
    def __init__(self, logger):
        self.logger = logger
    
    def validate_data_structure(self, data: List[Dict], pdf_url: str) -> List[Dict]:
        """Validate data structure and content quality"""
        if not data:
            raise DataValidationError(f"Empty dataset for {pdf_url}")
        
        validation_issues = []
        
        # Check for minimum data requirements
        if len(data) < 1:
            validation_issues.append("No data rows found")
        
        # Check column structure
        if not data[0]:
            validation_issues.append("No columns found in first row")
        else:
            columns = list(data[0].keys())
            
            # Check for suspicious column patterns
            if len(columns) < 2:
                validation_issues.append(f"Too few columns: {len(columns)}")
            
            # Check for empty column names
            empty_cols = [col for col in columns if not str(col).strip()]
            if empty_cols:
                validation_issues.append(f"Empty column names found: {len(empty_cols)}")
            
            # Check for data completeness
            total_cells = len(data) * len(columns)
            empty_cells = sum(1 for row in data for col, val in row.items() if not str(val).strip())
            
            if total_cells > 0:
                empty_percentage = empty_cells / total_cells
                if empty_percentage > 0.5:  # More than 50% empty
                    validation_issues.append(f"High percentage of empty cells: {empty_percentage:.1%}")
        
        # If critical issues found, raise error
        critical_issues = [issue for issue in validation_issues if any(keyword in issue.lower() 
                          for keyword in ['no data', 'no columns', 'too few columns'])]
        
        if critical_issues:
            raise DataValidationError(f"Critical validation issues for {pdf_url}: {'; '.join(critical_issues)}")
        
        # Log warnings for non-critical issues
        if validation_issues:
            self.logger.warning(f"Data validation warnings for {pdf_url}: {'; '.join(validation_issues)}")
        
        return data

class SmartFEDAOExtractor:
    """Enhanced FEDAO extractor with proper header detection and CSV generation."""

    def __init__(self, project_id: str, region: str):
        self.logger = logging.getLogger(__name__)
        
        # Initialize the Vertex AI client
        aiplatform.init(project=project_id, location=region)
        self.model = GenerativeModel("gemini-2.0-flash")
        
        # Initialize validator
        self.validator = ExtractionValidator(self.logger)

    def call_gemini_api(self, prompt: str, data: str) -> Optional[Dict]:
        """Call the model using the Vertex AI SDK."""
        try:
            full_prompt = f"{prompt}\n\nHTML/Data to process:\n{data}"
            
            response = self.model.generate_content(
                [full_prompt],
                generation_config={
                    "max_output_tokens": 8192,
                    "temperature": 0.1,
                    "response_mime_type": "application/json",
                },
            )
            
            content = response.text
            
            try:
                return json.loads(content)
            except json.JSONDecodeError:
                # Try to extract JSON from markdown
                json_match = re.search(r"```json\s*(\[.*\])\s*```", content, re.DOTALL)
                if json_match:
                    return json.loads(json_match.group(1))
                return {"text": content}

        except Exception as e:
            self.logger.error(f"Vertex AI call failed: {e}")
            raise IOError(f"Vertex AI call failed: {e}") from e

    def smart_html_preprocessing(self, html_content: str) -> List[str]:
        """Extract PDF links from HTML with configurable scope"""
        soup = BeautifulSoup(html_content, 'html.parser')
        pdf_links = []
        
        # Get extraction scope from environment variable
        extraction_scope = os.environ.get("EXTRACTION_SCOPE", "tables").lower()
        
        if extraction_scope == "tables":
            # Only look for PDFs within table elements
            tables = soup.find_all('table')
            
            for table in tables:
                links = table.find_all('a', href=True)
                for link in links:
                    href = link['href']
                    if href.lower().endswith('.pdf'):
                        if href.startswith('/'):
                            href = 'https://www.newyorkfed.org' + href
                        elif not href.startswith('http'):
                            href = 'https://www.newyorkfed.org/' + href
                        pdf_links.append(href)
        
        elif extraction_scope == "full":
            # Look for PDFs anywhere on the page
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.lower().endswith('.pdf'):
                    if href.startswith('/'):
                        href = 'https://www.newyorkfed.org' + href
                    elif not href.startswith('http'):
                        href = 'https://www.newyorkfed.org/' + href
                    pdf_links.append(href)
        
        elif extraction_scope == "specific":
            # Look for PDFs only in specific containers
            containers = soup.find_all(['div', 'section'], class_=lambda x: x and ('schedule' in x.lower() or 'operation' in x.lower()))
            
            for container in containers:
                links = container.find_all('a', href=True)
                for link in links:
                    href = link['href']
                    if href.lower().endswith('.pdf'):
                        if href.startswith('/'):
                            href = 'https://www.newyorkfed.org' + href
                        elif not href.startswith('http'):
                            href = 'https://www.newyorkfed.org/' + href
                        pdf_links.append(href)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_pdf_links = []
        for link in pdf_links:
            if link not in seen:
                seen.add(link)
                unique_pdf_links.append(link)
        
        return unique_pdf_links

    def pre_process_raw_table(self, table: List[List]) -> List[List]:
        """
        Adapts the logic from the local parser to merge split-rows before sending to AI.
        """
        if not table:
            return []

        processed_rows = []
        i = 0
        while i < len(table):
            row1 = table[i]
            row2 = table[i + 1] if (i + 1) < len(table) else []

            # Simple check to see if row1 looks like the start of a multi-line entry
            # A more robust check, like in your local parser, would be better.
            is_split_row = len(row1) > 2 and row1[0] and not row1[-1]

            if is_split_row and row2:
                # Combine row1 and row2 into a single, coherent row
                combined_row = []
                for j in range(max(len(row1), len(row2))):
                    cell1 = row1[j] if j < len(row1) else ''
                    cell2 = row2[j] if j < len(row2) else ''
                    # Combine the text from the two cells
                    combined_text = f"{cell1 or ''} {cell2 or ''}".strip()
                    combined_row.append(combined_text)
                
                processed_rows.append(combined_row)
                i += 2  # Skip the next row since we've merged it
            else:
                processed_rows.append(row1)
                i += 1
                
        return processed_rows
    def extract_text_from_pdf(self, pdf_url: str) -> str:
    """Extract table data from PDF using enhanced pdfplumber method with better table detection and fix row-overflows."""
    try:
        # Download the PDF
        response = requests.get(pdf_url, timeout=30)
        response.raise_for_status()

        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
            temp_file.write(response.content)
            temp_file.flush()

            # Extract table using pdfplumber with multiple strategies
            all_table_data = []

            with pdfplumber.open(temp_file.name) as pdf:
                for page_num, page in enumerate(pdf.pages):
                    self.logger.info(f"Processing page {page_num + 1} of PDF: {pdf_url}")

                    for strategy in [
                        {"vertical_strategy": "lines", "horizontal_strategy": "lines", "snap_tolerance": 3},
                        {"vertical_strategy": "text", "horizontal_strategy": "text", "snap_tolerance": 5},
                        {"vertical_strategy": "text", "horizontal_strategy": "text", "snap_tolerance": 10},
                    ]:
                        tables = page.extract_tables(strategy)
                        if tables:
                            largest_table = max(tables, key=lambda t: len(t) * len(t[0]) if t and t[0] else 0)
                            if largest_table and len(largest_table) > 1:
                                all_table_data = largest_table
                                break
                    if all_table_data:
                        break

            os.unlink(temp_file.name)

            if all_table_data:
                clean_table_data = self.pre_process_raw_table(all_table_data)
                csv_content = self.convert_table_to_csv_ai_enhanced(clean_table_data, pdf_url)

                # Post-processing step to fix row-overflow
                df = pd.read_csv(StringIO(csv_content), dtype=str).fillna('')
                df = self.fix_row_overflows(df)
                output_csv = StringIO()
                df.to_csv(output_csv, index=False)
                return output_csv.getvalue()
            else:
                self.logger.warning(f"No table data extracted from PDF: {pdf_url}")
                return ""

    except Exception as e:
        self.logger.error(f"Error extracting PDF {pdf_url}: {e}")
        return ""


def fix_row_overflows(self, df: pd.DataFrame) -> pd.DataFrame:
    """Post-processing function to fix rows where cell content overflows causing misalignment."""
    fixed_rows = []
    prev_row = None
    correct_length = len(df.columns)

    for _, row in df.iterrows():
        non_empty_cells = row.count()
        if non_empty_cells < correct_length:
            if prev_row is not None:
                prev_row = prev_row.combine_first(row)
                fixed_rows[-1] = prev_row
            else:
                fixed_rows.append(row)
        else:
            fixed_rows.append(row)
            prev_row = row

    return pd.DataFrame(fixed_rows)


def convert_table_to_csv_ai_enhanced(self, table: List[List], pdf_url: str) -> str:
    """
    Converts a raw table from pdfplumber into a clean CSV string using Vertex AI
    to semantically understand the header and data structure.
    """
    if not table or len(table) < 2:  # Need at least a potential header and data row
        self.logger.warning(f"Table from {pdf_url} is too small to process.")
        return ""

    # Convert the raw table to a JSON string for the model.
    # No need for heavy pre-processing; the model can handle messy data.
    raw_table_json = json.dumps(table, indent=2)

    # The prompt is crucial. We instruct the model to act as an expert
    # and define the exact JSON structure we want in return.
    prompt = f"""
You are an expert data extraction assistant. Your task is to analyze a **pre-processed and cleaned table** from a PDF and ensure it is correctly structured as clean JSON.

The provided table has already been processed to handle common structural issues like multi-line rows.

Here is the cleaned table data:
{raw_table_json}

Your instructions are:
1.  **Identify the Header:** The header should now be on a single, clear row. Identify it.
2.  **Validate Data Rows:** Ensure each data row has the same number of columns as the header.
3.  **Standardize Data:** Clean up any remaining artifacts (e.g., extra whitespace) and ensure data formats are consistent.
4.  **Output Format:** Your final output MUST be a single, valid JSON object with "headers" and "data" keys.

Now, process the provided clean table data and generate the structured JSON.
"""

    try:
        self.logger.info(f"Calling Vertex AI to process table from {pdf_url}")
        
        # Using the Vertex AI SDK to call the model
        response = self.model.generate_content(
            [prompt],
            generation_config={
                "max_output_tokens": 8192,
                "temperature": 0.0,  # Set to 0 for deterministic, structured output
                "response_mime_type": "application/json", # Ensures the model output is valid JSON
            },
        )
        
        # The response.text is now a guaranteed JSON string
        structured_result = json.loads(response.text)

        # --- Validation of the AI Model's Output ---
        if "headers" not in structured_result or "data" not in structured_result:
            self.logger.error(f"AI response for {pdf_url} is missing 'headers' or 'data' keys.")
            return ""

        headers = structured_result['headers']
        data_rows = structured_result['data']

        if not isinstance(headers, list) or not isinstance(data_rows, list):
            self.logger.error(f"AI response for {pdf_url} has incorrect data types for keys.")
            return ""

        self.logger.info(f"AI successfully processed table from {pdf_url}:")
        self.logger.info(f"  - Detected Headers: {headers}")
        self.logger.info(f"  - Detected Data Rows: {len(data_rows)}")

        # --- Convert the clean, structured data to CSV ---
        # This part is now simple and reliable because the AI did the hard work.
        output = StringIO()
        writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)
        
        writer.writerow(headers)
        writer.writerows(data_rows)
        
        csv_content = output.getvalue()
        return csv_content

    except Exception as e:
        self.logger.error(f"An error occurred calling Vertex AI or processing its response for {pdf_url}: {e}", exc_info=True)
        return ""

    def extract_with_ai_enhanced(self, html_content: str, table_type: str) -> List[Dict]:
        """Enhanced AI extraction with PROPER header handling for CSV conversion."""
        pdf_links = self.smart_html_preprocessing(html_content)
        
        if not pdf_links:
            self.logger.warning("No PDF links found in HTML content")
            return []

        # TEST MODE: Process only first PDF when enabled
        test_mode = os.environ.get("TEST_MODE", "true").lower() == "true"
        max_pdfs = 1 if test_mode else 3
        
        all_extracted_data = []
        
        # Process limited number of PDFs based on test mode
        for i, pdf_url in enumerate(pdf_links[:max_pdfs]):
            self.logger.info(f"Processing PDF {i+1}/{min(len(pdf_links), max_pdfs)}: {pdf_url}")
            
            pdf_csv_content = self.extract_text_from_pdf(pdf_url)
            if not pdf_csv_content:
                self.logger.warning(f"No content extracted from PDF: {pdf_url}")
                continue
            
            # CRITICAL: Properly parse CSV with headers
            try:
                # Use StringIO to read the CSV content properly
                csv_io = StringIO(pdf_csv_content)
                df = pd.read_csv(csv_io, dtype=str).fillna('')
                
                self.logger.info(f"Successfully parsed CSV from PDF:")
                self.logger.info(f"  - Headers: {list(df.columns)}")
                self.logger.info(f"  - Rows: {len(df)}")
                
                if len(df) > 0:
                    # Log first row for verification
                    self.logger.info(f"  - First row: {df.iloc[0].to_dict()}")
                    
                    # Convert DataFrame to list of dictionaries
                    pdf_extracted_data = df.to_dict('records')
                    self.logger.info(f"Extracted {len(pdf_extracted_data)} rows from PDF")
                    all_extracted_data.extend(pdf_extracted_data)
                else:
                    self.logger.warning(f"No data rows found in PDF: {pdf_url}")
                    
            except Exception as e:
                self.logger.error(f"Error parsing CSV from PDF {pdf_url}: {e}")
                self.logger.error(f"CSV content preview: {pdf_csv_content[:500]}")
                
                # Fallback: Try to parse manually
                try:
                    lines = pdf_csv_content.strip().split('\n')
                    if len(lines) >= 2:  # At least header + 1 data row
                        reader = csv.reader(lines)
                        rows = list(reader)
                        
                        if len(rows) > 0:
                            headers = rows[0]
                            data_rows = rows[1:]
                            
                            fallback_data = []
                            for row in data_rows:
                                # Ensure row has same length as headers
                                while len(row) < len(headers):
                                    row.append("")
                                row = row[:len(headers)]  # Trim if too long
                                
                                row_dict = dict(zip(headers, row))
                                fallback_data.append(row_dict)
                            
                            self.logger.info(f"Fallback parsing successful: {len(fallback_data)} rows")
                            all_extracted_data.extend(fallback_data)
                        
                except Exception as fallback_error:
                    self.logger.error(f"Fallback parsing also failed: {fallback_error}")
                    continue
            
            # In test mode, stop after first PDF
            if test_mode:
                break

        self.logger.info(f"Total extracted data: {len(all_extracted_data)} rows")
        
        # Log sample of extracted data for verification
        if all_extracted_data:
            self.logger.info(f"Sample extracted record: {all_extracted_data[0]}")
        
        return all_extracted_data

    def post_process_extracted_data(self, data: List[Dict], table_type: str) -> List[Dict]:
        """Post-process extracted data according to runbook requirements"""
        if not data:
            return data
            
        processed_data = []

        for row in data:
            processed_row = row.copy()
            
            # Clean up all text fields to remove indentation characters and commas
            for field_name, field_value in processed_row.items():
                if field_value and isinstance(field_value, str):
                    # Remove indentation characters and commas as per runbook
                    cleaned_value = re.sub(r'[,\t\n\r]', ' ', str(field_value))
                    cleaned_value = ' '.join(cleaned_value.split())  # Remove extra spaces
                    processed_row[field_name] = cleaned_value
            
            processed_data.append(processed_row)
        
        return processed_data


def scrape_treasury_operations_enhanced_ai(treasury_url: str, extractor: SmartFEDAOExtractor) -> List[Dict]:
    """Enhanced Treasury operations scraping that downloads PDFs from HTML page."""
    renderer_service_url = os.environ.get("RENDERER_SERVICE_URL", "http://your-renderer-service-url.a.run.app/render")

    render_config = {
        "url": treasury_url,
        "interactions_config": {
            "activate_tabs": True
        }
    }

    try:
        response = requests.post(renderer_service_url, json=render_config, timeout=60)
        response.raise_for_status()
        render_result = response.json()

        if 'error' in render_result or 'html' not in render_result:
            error_message = render_result.get('error', 'Unknown error from renderer')
            extractor.logger.error(f"Renderer service failed for {treasury_url}: {error_message}")
            return []

        html_content = render_result['html']

    except requests.exceptions.RequestException as e:
        extractor.logger.error(f"Failed to call renderer service for {treasury_url}: {e}")
        return []

    # Now extract from PDFs found in the HTML
    moa_data = extractor.extract_with_ai_enhanced(html_content, "MOA")
    standardized_moa = extractor.post_process_extracted_data(moa_data, "MOA")
    
    return standardized_moa

def scrape_ambs_operations_enhanced_ai(ambs_url: str, extractor: SmartFEDAOExtractor) -> List[Dict]:
    """Enhanced AMBS operations scraping that downloads PDFs from HTML page."""
    renderer_service_url = os.environ.get("RENDERER_SERVICE_URL", "http://your-renderer-service-url.a.run.app/render")

    render_config = {
        "url": ambs_url,
        "interactions_config": {
            "activate_tabs": True
        }
    }
    
    try:
        response = requests.post(renderer_service_url, json=render_config, timeout=60)
        response.raise_for_status()
        render_result = response.json()

        if 'error' in render_result or 'html' not in render_result:
            error_message = render_result.get('error', 'Unknown error from renderer')
            extractor.logger.error(f"Renderer service failed for {ambs_url}: {error_message}")
            return []

        html_content = render_result['html']

    except requests.exceptions.RequestException as e:
        extractor.logger.error(f"Failed to call renderer service for {ambs_url}: {e}")
        return []

    # Now extract from PDFs found in the HTML
    toa_data = extractor.extract_with_ai_enhanced(html_content, "TOA")
    standardized_toa = extractor.post_process_extracted_data(toa_data, "TOA")
    
    return standardized_toa


def upload_csv_to_gcs(csv_content: str, bucket_name: str, file_path: str, active_logger) -> str:
    """Upload CSV content to GCS"""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        blob.upload_from_string(csv_content, content_type='text/csv')
        
        return f"gs://{bucket_name}/{file_path}"
    except Exception as e:
        active_logger.error(f"Error uploading CSV to GCS gs://{bucket_name}/{file_path}: {str(e)}")
        raise


@functions_framework.cloud_event
def scrape_fedao_sources_ai(cloud_event):
    """AI-powered FEDAO source scraping with enhanced PDF extraction and proper header handling."""
    active_logger = setup_logging("simba", "fedao_project")
    active_logger.warning("Starting FEDAO source scraping with enhanced PDF extraction and header detection")

    try:
        customer_config = load_customer_config("simba")
        gcp_project_id = customer_config.get("gcp_project_id", os.environ.get("GCP_PROJECT"))
        region = os.environ.get("FUNCTION_REGION", "europe-west1")

        # Initialize the enhanced extractor
        extractor = SmartFEDAOExtractor(
            project_id=gcp_project_id, 
            region=region
        )

        treasury_url = os.environ.get("FEDAO_TREASURY_URL",
                                      "https://www.newyorkfed.org/markets/domestic-market-operations/monetary-policy-implementation/treasury-securities/treasury-securities-operational-details")
        ambs_url = os.environ.get("FEDAO_AMBS_URL",
                                  "https://www.newyorkfed.org/markets/ambs_operation_schedule#tabs-2")
        output_bucket = os.environ.get("FEDAO_OUTPUT_BUCKET", "execo-simba-fedao-poc")

        # --- MOA Data Extraction (Treasury Operations) ---
        moa_data = scrape_treasury_operations_enhanced_ai(treasury_url, extractor)
        moa_raw_file_path = f"FEDAO/inputs/raw_manual_uploads/FEDAO_MOA_RAW_DATA.csv"
        moa_file_url = ""

        if moa_data:
            moa_df = pd.DataFrame(moa_data)
            current_date = datetime.now().strftime('%Y%m%d')
            moa_df['Source_Date'] = current_date
            
            moa_csv_content = moa_df.to_csv(index=False)
            moa_file_url = upload_csv_to_gcs(moa_csv_content, output_bucket, moa_raw_file_path, active_logger)
            active_logger.info(f"MOA data uploaded: {len(moa_data)} rows with headers: {list(moa_df.columns)}")

        # --- TOA Data Extraction (AMBS Operations) ---
        toa_data = scrape_ambs_operations_enhanced_ai(ambs_url, extractor)
        toa_raw_file_path = f"FEDAO/inputs/raw_manual_uploads/FEDAO_TOA_RAW_DATA.csv"
        toa_file_url = ""

        if toa_data:
            toa_df = pd.DataFrame(toa_data)
            current_date = datetime.now().strftime('%Y%m%d')
            toa_df['Source_Date'] = current_date
            
            toa_csv_content = toa_df.to_csv(index=False)
            toa_file_url = upload_csv_to_gcs(toa_csv_content, output_bucket, toa_raw_file_path, active_logger)
            active_logger.info(f"TOA data uploaded: {len(toa_data)} rows with headers: {list(toa_df.columns)}")

        active_logger.warning(f"Extraction complete with enhanced header detection: MOA {len(moa_data) if moa_data else 0}, TOA {len(toa_data) if toa_data else 0}")
        
        return {
            "status": "success",
            "method": "Enhanced PDF extraction with proper header detection and CSV generation",
            "moa_rows_collected": len(moa_data) if moa_data else 0,
            "toa_rows_collected": len(toa_data) if toa_data else 0,
            "moa_raw_file": moa_file_url if moa_file_url else "No MOA file generated",
            "toa_raw_file": toa_file_url if toa_file_url else "No TOA file generated"
        }

    except Exception as e:
        active_logger.error(f"Critical error in scrape_fedao_sources_ai: {str(e)}", exc_info=True)
        raise