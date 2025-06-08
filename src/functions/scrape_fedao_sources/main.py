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
from src.common.utils import setup_logging
from src.common.config import load_customer_config, load_dynamic_site_config

# Initialize logger at the module level
logger = logging.getLogger(__name__)


class SmartFEDAOExtractor:
    """
    Enhanced FEDAO extractor using the Vertex AI SDK.
    """

    def __init__(self, project_id: str, region: str):
        # API Key is no longer needed.
        # self.api_key = ...
        # self.gemini_url = ...
        self.logger = logging.getLogger(__name__)
        
        # Initialize the Vertex AI client
        aiplatform.init(project=project_id, location=region)
        self.model = GenerativeModel("gemini-2.0-flash") # Or your preferred model

    def call_gemini_api(self, prompt: str, data: str) -> Optional[Dict]:
        """Call the model using the Vertex AI SDK."""
        try:
            full_prompt = f"{prompt}\n\nHTML/Data to process:\n{data}"
            
            # The SDK handles the request structure for you
            response = self.model.generate_content(
                [full_prompt],
                generation_config={
                    "max_output_tokens": 8192,
                    "temperature": 0.1,
                },
            )
            
            # The SDK also provides a safe way to access the text
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
            # Re-raise the exception to halt execution
            raise IOError(f"Vertex AI call failed: {e}") from e

    def smart_html_preprocessing(self, html_content: str) -> str:
        """Intelligently preprocess HTML to preserve important data"""
        soup = BeautifulSoup(html_content, 'html.parser')
        important_sections = []
        tables = soup.find_all('table')

        for table in tables:
            table_html = str(table)
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

        combined_html = '\n\n'.join(important_sections)

        if len(combined_html) > 25000:  # Increased limit
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
        """Enhanced AI extraction that fails explicitly if a valid list is not returned."""
        processed_html = self.smart_html_preprocessing(html_content)

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
        2. Examine headers carefully - they may have slight variations
        3. Map headers to expected fields using semantic similarity
        4. Extract ALL data rows
        5. For date ranges (e.g., "01/13/2025 - 01/17/2025"), extract the end date for OPERATION DATE.
        Output Format:
        Return a JSON array where each object represents one operation.
        Return ONLY the JSON array, no other text.
        """

        result = self.call_gemini_api(prompt, processed_html)

        if result and isinstance(result, list):
            self.logger.info(f"AI extracted {len(result)} {table_type} operations")
            return result

        # If we get here, the AI did not return a valid list.
        self.logger.error(f"AI extraction failed for {table_type}. API result: {result}")
        raise ValueError(f"AI extraction for {table_type} did not return a valid list.")

    def post_process_extracted_data(self, data: List[Dict], table_type: str) -> List[Dict]:
        """Post-process extracted data to fill missing fields intelligently"""
        processed_data = []

        for row in data:
            processed_row = row.copy()
            if table_type == "MOA" and processed_row.get("OPERATION DATE"):
                date_str = processed_row["OPERATION DATE"]
                if ' - ' in date_str:
                    parts = date_str.split(' - ')
                    if len(parts) == 2:
                        processed_row["OPERATION DATE"] = parts[1].strip()

            if table_type == "MOA" and not processed_row.get("OPERATION TIME(ET)"):
                for field_name, field_value in processed_row.items():
                    if field_value and isinstance(field_value, str):
                        time_match = re.search(r'(\d{1,2}:\d{2}(?:\s*[AP]M)?)', field_value, re.IGNORECASE)
                        if time_match:
                            processed_row["OPERATION TIME(ET)"] = time_match.group(1)
                            break

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


def scrape_treasury_operations_enhanced_ai(treasury_url: str, extractor: SmartFEDAOExtractor) -> List[Dict]:
    """Enhanced Treasury operations scraping with smart field detection"""
    extractor.logger.info(f"Enhanced AI scraping Treasury operations from: {treasury_url}")
    response = requests.get(treasury_url, timeout=30)
    response.raise_for_status()
    moa_data = extractor.extract_with_ai_enhanced(response.text, "MOA")
    standardized_moa = extractor.post_process_extracted_data(moa_data, "MOA")
    extractor.logger.info(f"Enhanced AI extracted {len(standardized_moa)} MOA operations")
    return standardized_moa


def scrape_ambs_operations_enhanced_ai(ambs_url: str, extractor: SmartFEDAOExtractor) -> List[Dict]:
    """Enhanced AMBS operations scraping with smart field detection"""
    extractor.logger.info(f"Enhanced AI scraping AMBS operations from: {ambs_url}")
    response = requests.get(ambs_url, timeout=30)
    response.raise_for_status()
    toa_data = extractor.extract_with_ai_enhanced(response.text, "TOA")
    standardized_toa = extractor.post_process_extracted_data(toa_data, "TOA")
    extractor.logger.info(f"Enhanced AI extracted {len(standardized_toa)} TOA operations")
    return standardized_toa


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
    """AI-only FEDAO source scraping and CSV generation."""
    active_logger = setup_logging("simba", "fedao_project")
    active_logger.info("AI-only Cloud Function 'scrape_fedao_sources_ai' triggered.")

    try:
        customer_config = load_customer_config("simba")
        gcp_project_id = customer_config.get("gcp_project_id", os.environ.get("GCP_PROJECT"))
        region = os.environ.get("FUNCTION_REGION", "europe-west1") # Get region from environment
        active_logger.info(f"Using GCP Project ID: {gcp_project_id} in region: {region}")

        # Initialize the extractor with project and region instead of an API key
        extractor = SmartFEDAOExtractor(project_id=gcp_project_id, region=region)

        treasury_url = os.environ.get("FEDAO_TREASURY_URL",
                                      "https://www.newyorkfed.org/markets/domestic-market-operations/monetary-policy-implementation/treasury-securities/treasury-securities-operational-details")
        ambs_url = os.environ.get("FEDAO_AMBS_URL",
                                  "https://www.newyorkfed.org/markets/ambs_operation_schedule#tabs-2")
        output_bucket = os.environ.get("FEDAO_OUTPUT_BUCKET", "execo-simba-fedao-poc")
        active_logger.info(f"Output GCS bucket: {output_bucket}")

        # --- MOA Data Extraction ---
        active_logger.info(">>> RUNNING AI-POWERED SCRAPING PATH FOR MOA <<<")
        moa_data = scrape_treasury_operations_enhanced_ai(treasury_url, extractor)
        moa_raw_file_path = f"FEDAO/inputs/raw_manual_uploads/FEDAO_MOA_RAW_DATA.csv"
        moa_file_url = ""

        if moa_data:
            moa_df = pd.DataFrame(moa_data)
            active_logger.info(f"AI collected {len(moa_df)} rows for MOA.")
            current_date = datetime.now().strftime('%Y%m%d')
            moa_df['Source_Date'] = current_date
            moa_csv_content = moa_df.to_csv(index=False)
            moa_file_url = upload_csv_to_gcs(moa_csv_content, output_bucket, moa_raw_file_path, active_logger)
        else:
            active_logger.warning("AI extraction for MOA completed successfully but returned no data.")

        # --- TOA Data Extraction ---
        active_logger.info(">>> RUNNING AI-POWERED SCRAPING PATH FOR TOA <<<")
        toa_data = scrape_ambs_operations_enhanced_ai(ambs_url, extractor)
        toa_raw_file_path = f"FEDAO/inputs/raw_manual_uploads/FEDAO_TOA_RAW_DATA.csv"
        toa_file_url = ""

        if toa_data:
            toa_df = pd.DataFrame(toa_data)
            active_logger.info(f"AI collected {len(toa_df)} rows for TOA.")
            current_date = datetime.now().strftime('%Y%m%d')
            toa_df['Source_Date'] = current_date
            toa_csv_content = toa_df.to_csv(index=False)
            toa_file_url = upload_csv_to_gcs(toa_csv_content, output_bucket, toa_raw_file_path, active_logger)
        else:
            active_logger.warning("AI extraction for TOA completed successfully but returned no data.")

        active_logger.info("AI-powered FEDAO source scraping completed successfully.")
        return {
            "status": "success",
            "method": "AI-powered extraction",
            "moa_rows_collected": len(moa_data) if moa_data else 0,
            "toa_rows_collected": len(toa_data) if toa_data else 0,
            "moa_raw_file": moa_file_url if moa_file_url else "No MOA file generated",
            "toa_raw_file": toa_file_url if toa_file_url else "No TOA file generated"
        }

    except Exception as e:
        active_logger.error(f"Critical error in 'scrape_fedao_sources_ai' function: {str(e)}", exc_info=True)
        # Re-raise the exception to ensure the Cloud Function is marked as a failure
        raise