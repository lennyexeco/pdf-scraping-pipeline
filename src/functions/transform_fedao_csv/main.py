# src/functions/transform_fedao_csv/main.py

import json
import logging
import os
import csv
from io import StringIO
from datetime import datetime
import re
import pandas as pd
# 'requests' is no longer needed for the AI call, but might be used elsewhere.
# If not, it can be removed from requirements.txt.
import requests
from typing import Dict, List, Any, Optional, Tuple

from google.cloud import storage, firestore, aiplatform
import functions_framework
from vertexai.generative_models import GenerativeModel

from src.common.utils import setup_logging
from src.common.config import load_customer_config, load_dynamic_site_config

# Initialize logger at the module level
logger = logging.getLogger(__name__)

class AIDataExtractor:
    """AI-powered data extraction and mapping for FEDAO using the Vertex AI SDK."""

    def __init__(self, project_id: str, region: str):
        # API Key and URL are no longer needed
        self.logger = logging.getLogger(__name__)
        
        # Initialize the Vertex AI client
        aiplatform.init(project=project_id, location=region)
        self.model = GenerativeModel("gemini-2.0-flash") # Or your preferred model

        # Schemas remain the same
        self.moa_schema = {
            "OPERATION DATE": "string - single date in YYYY-MM-DD format",
            "OPERATION TIME(ET)": "string - time in HH:MM format with ET timezone",
            "SETTLEMENT DATE": "string - single date in YYYY-MM-DD format",
            "OPERATION TYPE": "string - type of operation (e.g., Purchase, Sale, Repo)",
            "SECURITY TYPE AND MATURITY": "string - security type and maturity info",
            "MATURITY RANGE": "string - maturity range if specified",
            "MAXIMUM OPERATION CURRENCY": "string - currency symbol (e.g., $, €)",
            "MAXIMUMOPERATIONSIZE": "float - numeric value only",
            "MAXIMUM OPERATION MULTIPLIER": "string - multiplier (million, billion, etc.)",
            "Source_Date": "string - date in YYYYMMDD format"
        }
        self.toa_schema = {
            "DATE": "string - single date in YYYY-MM-DD format",
            "OPERATION TYPE": "string - type of operation",
            "SECURITY TYPE AND MATURITY": "string - security type and maturity info",
            "CUSIP": "string - CUSIP identifier",
            "MAXIMUM PURCHASE AMOUNT": "string - purchase amount with currency and multiplier",
            "Source_Date": "string - date in YYYYMMDD format"
        }

    def call_gemini_api(self, prompt: str, data_sample: str) -> Optional[Dict]:
        """Call the model using the Vertex AI SDK."""
        try:
            full_prompt = f"{prompt}\n\nData to process:\n{data_sample}"
            
            response = self.model.generate_content(
                [full_prompt],
                generation_config={
                    "max_output_tokens": 4096,
                    "temperature": 0.1,
                },
            )
            
            content = response.text
            try:
                # The model often returns JSON wrapped in markdown, so we extract it.
                json_match = re.search(r"```json\s*(.*?)\s*```", content, re.DOTALL)
                if json_match:
                    return json.loads(json_match.group(1))
                # Fallback for raw JSON
                return json.loads(content)
            except json.JSONDecodeError:
                return {"text": content} # Return as text if it's not valid JSON

        except Exception as e:
            self.logger.error(f"Vertex AI call failed: {e}", exc_info=True)
            raise IOError(f"Vertex AI call failed: {e}") from e

    def intelligent_column_mapping(self, df: pd.DataFrame, target_schema: Dict) -> Dict[str, str]:
        """Use AI to intelligently map source columns to target schema."""
        sample_data = {
            "columns": df.columns.tolist(),
            "sample_rows": df.head(3).to_dict('records'),
            "target_schema": target_schema
        }
        prompt = f"""
        You are an expert data analyst. Map the source CSV columns to the target schema.
        Target Schema:
        {json.dumps(target_schema, indent=2)}
        Instructions:
        1. Analyze the source columns and sample data.
        2. Map each source column to the most appropriate target schema field.
        3. If no source column matches a target field, map it to "MISSING".
        4. Return ONLY a JSON object with the mapping.
        """
        result = self.call_gemini_api(prompt, json.dumps(sample_data, indent=2))
        if result and isinstance(result, dict) and 'text' not in result:
            return result
        
        self.logger.error(f"AI column mapping failed. API result: {result}")
        raise ValueError("AI column mapping did not return a valid dictionary.")

    def intelligent_data_transformation(self, df: pd.DataFrame, column_mapping: Dict, target_schema: Dict, data_type: str) -> pd.DataFrame:
        """Use AI to intelligently transform and clean data."""
        transformed_df = pd.DataFrame()
        for target_col, source_col in column_mapping.items():
            if source_col != "MISSING" and source_col in df.columns:
                transformed_df[target_col] = df[source_col].copy()
            else:
                transformed_df[target_col] = ""

        if not transformed_df.empty:
            transformed_df = self._ai_clean_data(transformed_df, target_schema, data_type)
        
        return transformed_df

    def _ai_clean_data(self, df: pd.DataFrame, target_schema: Dict, data_type: str) -> pd.DataFrame:
        """Use AI to clean and standardize data values, failing explicitly if a batch fails."""
        batch_size = 10
        cleaned_df = df.copy()
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            prompt = f"""
            You are a data cleaning expert. Clean and standardize this {data_type} data batch according to the schema.
            Schema Requirements:
            {json.dumps(target_schema, indent=2)}
            Cleaning Rules:
            1. Extract single dates from date ranges (use the later/end date).
            2. Parse operation amounts into currency, numeric value, and multiplier.
            3. Standardize time formats to HH:MM.
            4. Clean up text fields (trim whitespace).
            5. Ensure numeric fields are numbers. Handle missing/empty values appropriately.
            Return the cleaned data as a JSON array of objects.
            """
            batch_data = batch.to_dict('records')
            result = self.call_gemini_api(prompt, json.dumps(batch_data, indent=2))
            if result and isinstance(result, list):
                for j, cleaned_row in enumerate(result):
                    if i + j < len(cleaned_df):
                        for col, value in cleaned_row.items():
                            if col in cleaned_df.columns:
                                cleaned_df.iloc[i + j, cleaned_df.columns.get_loc(col)] = value
            else:
                self.logger.error(f"AI data cleaning failed for a batch. API result: {result}")
                raise ValueError("AI data cleaning failed for a batch and did not return a list.")
        return cleaned_df

def determine_source_date_ai(df: pd.DataFrame, extractor: AIDataExtractor) -> str:
    """Use AI to determine the appropriate source date."""
    date_columns = [col for col in df.columns if 'date' in col.lower()]
    sample_data = {
        "date_columns": date_columns,
        "sample_dates": {col: df[col].dropna().head(5).tolist() for col in date_columns}
    }
    prompt = """
    Analyze the date columns and determine the most appropriate date to use as Source_Date.
    Rules:
    1. Use the latest/most recent operation date.
    2. If date ranges exist, use the end date.
    3. Return the date in YYYYMMDD format.
    4. Return only the date string, no other text.
    """
    result = extractor.call_gemini_api(prompt, json.dumps(sample_data, indent=2))
    if result and isinstance(result, dict) and 'text' in result:
        date_match = re.search(r'(\d{8})', result['text'])
        if date_match:
            return date_match.group(0)
    logger.error(f"AI could not determine a valid Source_Date. API result: {result}")
    raise ValueError("AI could not determine a valid Source_Date.")

def create_default_project_config():
    """Create default project configuration for FEDAO."""
    return {
        "fedao_input_configs": {
            "FEDAO_MOA_RAW_DATA": {
                "gcs_processed_path_root": "FEDAO/processed_csvs",
                "gcs_incremental_path_root": "FEDAO/master_data", 
                "master_filename_template": "FEDAO_MOA_DATA.csv"
            },
            "FEDAO_TOA_RAW_DATA": {
                "gcs_processed_path_root": "FEDAO/processed_csvs",
                "gcs_incremental_path_root": "FEDAO/master_data",
                "master_filename_template": "FEDAO_TOA_DATA.csv"
            }
        }
    }

@functions_framework.cloud_event
def transform_fedao_csv_ai(cloud_event):
    """AI-only FEDAO CSV transformation function using Vertex AI. Fails on error."""
    try:
        gcs_event_data = cloud_event.data
        bucket_name = gcs_event_data["bucket"]
        file_name = gcs_event_data["name"]

        if 'raw_manual_uploads' not in file_name or not file_name.lower().endswith('.csv') or '.keep' in file_name.lower():
            logging.info(f"Ignoring file: {file_name}")
            return {"status": "ignored", "message": "File is not a target raw CSV"}

        customer_id = os.environ.get("CUSTOMER_ID_FOR_FEDAO", "simba")
        project_config_name = "fedao_project"
        active_logger = setup_logging(customer_id, project_config_name)
        active_logger.info(f"Processing file with Vertex AI: gs://{bucket_name}/{file_name}")

        customer_config = load_customer_config(customer_id)
        gcp_project_id = customer_config.get("gcp_project_id", os.environ.get("GCP_PROJECT"))
        region = os.environ.get("FUNCTION_REGION", "europe-west1")

        # Initialize the extractor with project and region for Vertex AI
        extractor = AIDataExtractor(project_id=gcp_project_id, region=region)
        
        # ... rest of the function remains the same ...
        
        try:
            db = firestore.Client(project=gcp_project_id)
            project_config = load_dynamic_site_config(db, project_config_name, active_logger)
        except Exception as e:
            active_logger.warning(f"Could not load project config from Firestore: {e}. Using defaults.")
            project_config = create_default_project_config()

        raw_filename_base = os.path.basename(file_name)
        if "FEDAO_MOA_RAW_DATA" in raw_filename_base:
            data_type = "MOA"
            target_schema = extractor.moa_schema
            config_key_prefix = "FEDAO_MOA_RAW_DATA"
        elif "FEDAO_TOA_RAW_DATA" in raw_filename_base:
            data_type = "TOA"
            target_schema = extractor.toa_schema
            config_key_prefix = "FEDAO_TOA_RAW_DATA"
        else:
            raise ValueError(f"Cannot determine data type from filename: {raw_filename_base}")

        input_config = project_config["fedao_input_configs"][config_key_prefix]
        storage_client = storage.Client(project=gcp_project_id)
        source_bucket_obj = storage_client.bucket(bucket_name)
        source_blob = source_bucket_obj.blob(file_name)
        
        if not source_blob.exists():
            raise FileNotFoundError(f"Source file not found: gs://{bucket_name}/{file_name}")
        
        csv_content_str = source_blob.download_as_text()
        if not csv_content_str.strip():
            active_logger.warning("Source file is empty, no action taken.")
            return {"status": "success", "message": "Source file was empty"}

        df = pd.read_csv(StringIO(csv_content_str), dtype=str).fillna('')
        active_logger.info(f"Read {len(df)} rows from source. Columns: {df.columns.tolist()}")

        active_logger.info(f"Using AI to map columns for {data_type} data...")
        column_mapping = extractor.intelligent_column_mapping(df, target_schema)
        active_logger.info(f"AI column mapping: {column_mapping}")
        
        active_logger.info("Applying AI-powered data transformation...")
        transformed_df = extractor.intelligent_data_transformation(df, column_mapping, target_schema, data_type)
        
        source_date = determine_source_date_ai(df, extractor)
        transformed_df['Source_Date'] = source_date
        
        active_logger.info(f"Transformed data shape: {transformed_df.shape}. Final columns: {transformed_df.columns.tolist()}")
        
        ts_suffix = datetime.now().strftime('%Y%m%d_%H%M%S')
        processed_csv_filename = f"FEDAO_AI_PROCESSED_{config_key_prefix}_{ts_suffix}.csv"
        processed_csv_gcs_path = os.path.join(input_config["gcs_processed_path_root"], datetime.now().strftime('%Y%m%d'), processed_csv_filename)
        
        processed_blob = source_bucket_obj.blob(processed_csv_gcs_path)
        processed_csv_content = transformed_df.to_csv(index=False, quoting=csv.QUOTE_MINIMAL, na_rep='')
        processed_blob.upload_from_string(processed_csv_content, content_type='text/csv')
        active_logger.info(f"Uploaded AI-processed CSV: gs://{bucket_name}/{processed_csv_gcs_path}")
        
        master_filename = input_config.get("master_filename_template")
        master_file_gcs_path = "Not configured"
        if master_filename:
            incremental_master_path = os.path.join(input_config["gcs_incremental_path_root"], master_filename)
            master_blob = source_bucket_obj.blob(incremental_master_path)
            
            if master_blob.exists():
                existing_content = master_blob.download_as_text()
                if existing_content.strip():
                    existing_df = pd.read_csv(StringIO(existing_content), dtype=str).fillna('')
                    all_cols = list(set(transformed_df.columns) | set(existing_df.columns))
                    combined_df = pd.concat([
                        existing_df.reindex(columns=all_cols, fill_value=''), 
                        transformed_df.reindex(columns=all_cols, fill_value='')
                    ], ignore_index=True)
                else:
                    combined_df = transformed_df
            else:
                combined_df = transformed_df

            dedup_cols_map = {
                "FEDAO_MOA_RAW_DATA": ['OPERATION DATE', 'OPERATION TYPE', 'SECURITY TYPE AND MATURITY'],
                "FEDAO_TOA_RAW_DATA": ['DATE', 'OPERATION TYPE', 'CUSIP']
            }
            available_dedup_cols = [col for col in dedup_cols_map[config_key_prefix] if col in combined_df.columns]
            
            initial_count = len(combined_df)
            combined_df.drop_duplicates(subset=available_dedup_cols, keep='last', inplace=True)
            active_logger.info(f"Deduplicated master: {initial_count} -> {len(combined_df)} rows")
            
            master_csv_content = combined_df.to_csv(index=False, quoting=csv.QUOTE_MINIMAL, na_rep='')
            master_blob.upload_from_string(master_csv_content, content_type='text/csv')
            master_file_gcs_path = f"gs://{bucket_name}/{incremental_master_path}"
            active_logger.info(f"Updated master file: {master_file_gcs_path} ({len(combined_df)} rows)")

        return {
            "status": "success",
            "processing_method": "AI-only (Vertex AI)",
            "transformed_rows": len(transformed_df),
            "processed_snapshot_gcs_path": f"gs://{bucket_name}/{processed_csv_gcs_path}",
            "master_incremental_gcs_path": master_file_gcs_path
        }
        
    except Exception as e:
        final_logger = logging.getLogger(__name__)
        final_logger.error(f"Transform function failed critically: {e}", exc_info=True)
        raise