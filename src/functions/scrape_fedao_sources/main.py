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
                csv_content = self.convert_table_to_csv_improved(all_table_data, pdf_url)

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


def convert_table_to_csv_improved(self, table: List[List], pdf_url: str) -> str:
    """Improved table to CSV conversion with better header detection and cell boundary handling"""
    if not table or len(table) == 0:
        return ""

    self.logger.info(f"Processing table with {len(table)} rows from {pdf_url}")

    # Step 1: Clean and normalize all cells
    cleaned_table = []
    max_cols = 0

    for row_idx, row in enumerate(table):
        if row is None:
            continue

        cleaned_row = []
        for cell in row:
            if cell is not None:
                cell_content = str(cell).strip()
                cell_content = re.sub(r'\s+', ' ', cell_content)
                cell_content = re.sub(r'[,\t\n\r]', ' ', cell_content)
                cell_content = cell_content.strip()
                cleaned_row.append(cell_content)
            else:
                cleaned_row.append("")

        if any(cell.strip() for cell in cleaned_row):
            cleaned_table.append(cleaned_row)
            max_cols = max(max_cols, len(cleaned_row))

    if not cleaned_table:
        self.logger.warning(f"No valid data rows found in table from {pdf_url}")
        return ""

    # Step 2: Normalize all rows to have the same number of columns
    normalized_table = []
    for row in cleaned_table:
        normalized_row = row[:]
        while len(normalized_row) < max_cols:
            normalized_row.append("")
        normalized_table.append(normalized_row[:max_cols])

    # Step 3: Smart header detection with multi-line support
    header_row_indices = []
    fed_header_keywords = [
        'operation date', 'operation time', 'settlement date', 'operation type',
        'security type', 'maturity', 'maximum', 'size', 'cusip', 'securities included',
        'security maximums', 'operation maximum', 'date', 'time', 'type'
    ]

    for i, row in enumerate(normalized_table[:5]):  # Check first 5 rows
        row_text = ' '.join(row).lower()
        keyword_matches = sum(1 for keyword in fed_header_keywords if keyword in row_text)
        if keyword_matches > 0:
            header_row_indices.append(i)

    header_to_use = None
    data_rows_start_index = 0

    # Check for consecutive header rows to merge
    if len(header_row_indices) > 1 and all(header_row_indices[i] == header_row_indices[i-1] + 1 for i in range(1, len(header_row_indices))):
        self.logger.info(f"Detected {len(header_row_indices)} consecutive header rows. Merging them.")
        merged_header = [""] * max_cols
        for col_idx in range(max_cols):
            header_parts = [normalized_table[row_idx][col_idx] for row_idx in header_row_indices if col_idx < len(normalized_table[row_idx])]
            merged_header[col_idx] = " ".join(part for part in header_parts if part)

        header_to_use = merged_header
        data_rows_start_index = max(header_row_indices) + 1

    elif header_row_indices: # Single header row detected
        self.logger.info(f"Detected a single header row at index {header_row_indices[0]}.")
        header_to_use = normalized_table[header_row_indices[0]]
        data_rows_start_index = header_row_indices[0] + 1
    
    else: # No header detected, use generic headers
        self.logger.info("No header row detected. Using generic headers.")
        header_to_use = [f"COLUMN_{i+1}" for i in range(max_cols)]
        data_rows_start_index = 0

    # Clean up the final header
    cleaned_headers = []
    for header in header_to_use:
        clean_header = header.strip().upper()
        clean_header = re.sub(r'[^\w\s()-]', ' ', clean_header)
        clean_header = re.sub(r'\s+', ' ', clean_header).strip()
        cleaned_headers.append(clean_header if clean_header else f"COLUMN_{len(cleaned_headers) + 1}")

    final_table = [cleaned_headers] + normalized_table[data_rows_start_index:]
    
    # Step 5: Final validation and cleanup
    if len(final_table) < 2:
        self.logger.warning(f"Table too small after processing: {len(final_table)} rows")
        return ""

    # Remove completely empty data rows
    header = final_table[0]
    clean_data_rows = [row for row in final_table[1:] if any(cell.strip() for cell in row)]

    if not clean_data_rows:
        self.logger.warning(f"No valid data rows found after cleanup")
        return ""

    final_table = [header] + clean_data_rows

    # Step 6: Convert to CSV
    output = StringIO()
    writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)
    writer.writerows(final_table)
    csv_content = output.getvalue()

    self.logger.info(f"Generated CSV from {pdf_url}:")
    self.logger.info(f"  - Headers: {header}")
    self.logger.info(f"  - Data rows: {len(clean_data_rows)}")
    self.logger.info(f"  - Columns: {len(header)}")

    return csv_content

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