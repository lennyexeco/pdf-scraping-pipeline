import json
import logging
import os
import csv
from io import StringIO
from datetime import datetime
import re
import pandas as pd
import requests
from typing import Dict, List, Any, Optional, Tuple

from google.cloud import storage, firestore
import functions_framework

# Initialize logger at the module level
logger = logging.getLogger(__name__)

# INLINE UTILITY FUNCTIONS
def setup_logging(customer_id: str, project_name: str):
    """Setup logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
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
    try:
        doc_ref = db.collection('project_configs').document(project_name)
        doc = doc_ref.get()
        if doc.exists:
            return doc.to_dict()
    except Exception as e:
        logger.warning(f"Could not load config from Firestore: {e}")
    
    return create_default_project_config()

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

class FEDAODataProcessor:
    """Simplified FEDAO data processor that works directly with extracted CSV data."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def process_moa_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process MOA data with minimal transformations and MAXIMUM OPERATION SIZE splitting."""
        self.logger.info(f"Processing MOA data: {len(df)} rows, {len(df.columns)} columns")
        self.logger.info(f"Original columns: {list(df.columns)}")
        
        # Work with a copy
        processed_df = df.copy()
        
        # Apply basic text cleaning to all columns
        for col in processed_df.columns:
            processed_df[col] = processed_df[col].apply(self._clean_text)
        
        # Find the column that contains operation size/amount information
        size_column = self._find_operation_size_column(processed_df)
        
        if size_column:
            self.logger.info(f"Found operation size column: {size_column}")
            
            # Split the operation size column according to runbook requirements
            currency_data, size_data, multiplier_data = self._split_operation_size_column(
                processed_df[size_column]
            )
            
            # Add the new columns
            processed_df['MAXIMUM OPERATION CURRENCY'] = currency_data
            processed_df['MAXIMUM OPERATION SIZE'] = size_data
            processed_df['MAXIMUM OPERATION MULTIPLIER'] = multiplier_data
            
            # Remove the original combined column if it's not one of our target columns
            if size_column not in ['MAXIMUM OPERATION CURRENCY', 'MAXIMUM OPERATION SIZE', 'MAXIMUM OPERATION MULTIPLIER']:
                processed_df = processed_df.drop(columns=[size_column])
                self.logger.info(f"Removed original size column: {size_column}")
        else:
            self.logger.warning("No operation size column found for splitting")
            # Add empty columns to maintain schema
            processed_df['MAXIMUM OPERATION CURRENCY'] = ''
            processed_df['MAXIMUM OPERATION SIZE'] = ''
            processed_df['MAXIMUM OPERATION MULTIPLIER'] = ''
        
        # Clean dates
        for col in processed_df.columns:
            if 'date' in col.lower():
                processed_df[col] = processed_df[col].apply(self._clean_date)
            elif 'time' in col.lower():
                processed_df[col] = processed_df[col].apply(self._clean_time)
        
        self.logger.info(f"Processed MOA data: {len(processed_df)} rows, {len(processed_df.columns)} columns")
        self.logger.info(f"Final columns: {list(processed_df.columns)}")
        
        return processed_df

    def process_toa_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process TOA data with minimal transformations and empty column removal."""
        self.logger.info(f"Processing TOA data: {len(df)} rows, {len(df.columns)} columns")
        self.logger.info(f"Original columns: {list(df.columns)}")
        
        # Work with a copy
        processed_df = df.copy()
        
        # Step 1: Identify and remove completely empty columns first
        empty_cols = []
        for col in processed_df.columns:
            # Check if column is completely empty or contains only whitespace
            col_values = processed_df[col].astype(str).str.strip()
            if col_values.eq('').all() or col_values.eq('nan').all():
                empty_cols.append(col)
        
        if empty_cols:
            processed_df = processed_df.drop(columns=empty_cols)
            self.logger.info(f"Removed completely empty columns: {empty_cols}")
        
        # Step 2: Identify and remove columns with only header-like content (single short values)
        near_empty_cols = []
        for col in processed_df.columns:
            col_values = processed_df[col].astype(str).str.strip()
            non_empty_values = col_values[col_values != '']
            
            # If column has very few non-empty values and they're all very short
            if len(non_empty_values) <= 1 and all(len(val) <= 3 for val in non_empty_values):
                near_empty_cols.append(col)
        
        if near_empty_cols:
            processed_df = processed_df.drop(columns=near_empty_cols)
            self.logger.info(f"Removed near-empty columns: {near_empty_cols}")
        
        # Step 3: Apply basic text cleaning to remaining columns
        for col in processed_df.columns:
            processed_df[col] = processed_df[col].apply(self._clean_text)
        
        # Step 4: Clean dates and times
        for col in processed_df.columns:
            if 'date' in col.lower():
                processed_df[col] = processed_df[col].apply(self._clean_date)
            elif 'time' in col.lower():
                processed_df[col] = processed_df[col].apply(self._clean_time)
        
        # Step 5: Final check for columns that became empty after cleaning
        final_empty_cols = []
        for col in processed_df.columns:
            col_values = processed_df[col].astype(str).str.strip()
            if col_values.eq('').all():
                final_empty_cols.append(col)
        
        if final_empty_cols:
            processed_df = processed_df.drop(columns=final_empty_cols)
            self.logger.info(f"Removed columns that became empty after cleaning: {final_empty_cols}")
        
        self.logger.info(f"Processed TOA data: {len(processed_df)} rows, {len(processed_df.columns)} columns")
        self.logger.info(f"Final columns: {list(processed_df.columns)}")
        
        return processed_df

    def _find_operation_size_column(self, df: pd.DataFrame) -> Optional[str]:
        """Find the column that contains operation size/amount information."""
        
        # Look for columns with keywords related to operation size
        size_keywords = [
            'maximum operation size',
            'max operation size', 
            'operation size',
            'maximum size',
            'max size',
            'amount',
            'maximum amount',
            'max amount'
        ]
        
        for col in df.columns:
            col_lower = col.lower().strip()
            for keyword in size_keywords:
                if keyword in col_lower:
                    return col
        
        # Look for columns that contain currency symbols and amounts
        for col in df.columns:
            sample_values = df[col].dropna().head(5)
            for value in sample_values:
                value_str = str(value).strip()
                # Check if value contains currency and amount patterns
                if re.search(r'[\$€£¥].*\d', value_str) or re.search(r'\d.*(?:million|billion|trillion)', value_str, re.IGNORECASE):
                    return col
        
        return None

    def _split_operation_size_column(self, series: pd.Series) -> Tuple[List[str], List[str], List[str]]:
        """Split operation size column into currency, size, and multiplier with enhanced pattern matching."""
        
        currency_data = []
        size_data = []
        multiplier_data = []
        
        for value in series:
            if pd.isna(value) or not str(value).strip():
                currency_data.append('')
                size_data.append('')
                multiplier_data.append('')
                continue
            
            value_str = str(value).strip()
            self.logger.debug(f"Processing operation size value: '{value_str}'")
            
            # Extract currency (symbols at the beginning)
            currency_match = re.match(r'^([^\d\s]*)', value_str)
            currency = currency_match.group(1).strip() if currency_match and currency_match.group(1).strip() else ''
            
            # Extract numeric value (including decimals and commas)
            numeric_match = re.search(r'([\d,]+\.?\d*)', value_str)
            numeric_value = numeric_match.group(1).replace(',', '') if numeric_match else ''
            
            # Enhanced multiplier extraction with better pattern matching
            multiplier = ''
            
            # Look for complete words first, then partial matches
            multiplier_patterns = [
                # Complete words (case insensitive)
                (r'\b(trillion|trillions)\b', 'Trillion'),
                (r'\b(billion|billions)\b', 'Billion'), 
                (r'\b(million|millions)\b', 'Million'),
                (r'\b(thousand|thousands)\b', 'Thousand'),
                # Abbreviated forms
                (r'\b(tn|tri)\b', 'Trillion'),
                (r'\b(bn|bil)\b', 'Billion'),
                (r'\b(mn|mil)\b', 'Million'),
                (r'\b(k|th)\b', 'Thousand'),
                # Single letters at word boundaries
                (r'\bt\b', 'Trillion'),
                (r'\bb\b', 'Billion'),
                (r'\bm\b', 'Million'),
                # Partial word matches (for truncated text like "Millio")
                (r'millio[n]?', 'Million'),
                (r'billio[n]?', 'Billion'),
                (r'trillio[n]?', 'Trillion'),
                (r'thousan[d]?', 'Thousand'),
            ]
            
            # Try each pattern
            for pattern, standard_form in multiplier_patterns:
                if re.search(pattern, value_str, re.IGNORECASE):
                    multiplier = standard_form
                    break
            
            # If no standard multiplier found, check for any remaining text that might be a truncated multiplier
            if not multiplier:
                # Remove currency and number, see what's left
                remaining_text = value_str
                if currency:
                    remaining_text = remaining_text.replace(currency, '', 1).strip()
                if numeric_value:
                    remaining_text = re.sub(r'[\d,]+\.?\d*', '', remaining_text).strip()
                
                if remaining_text:
                    # Try to match partial multipliers
                    remaining_lower = remaining_text.lower()
                    if 'mill' in remaining_lower or 'mil' in remaining_lower:
                        multiplier = 'Million'
                    elif 'bill' in remaining_lower or 'bil' in remaining_lower:
                        multiplier = 'Billion'
                    elif 'trill' in remaining_lower or 'tri' in remaining_lower:
                        multiplier = 'Trillion'
                    elif 'thous' in remaining_lower or 'th' in remaining_lower:
                        multiplier = 'Thousand'
                    
                    self.logger.debug(f"Found partial multiplier '{remaining_text}' -> '{multiplier}'")
            
            currency_data.append(currency)
            size_data.append(numeric_value)
            multiplier_data.append(multiplier)
            
            self.logger.debug(f"Split '{value_str}' -> Currency: '{currency}', Size: '{numeric_value}', Multiplier: '{multiplier}'")
        
        return currency_data, size_data, multiplier_data

    def _clean_text(self, text_value):
        """Clean text according to runbook: remove commas and indentation characters."""
        if pd.isna(text_value) or not text_value:
            return ""
        
        text_str = str(text_value)
        
        # Remove commas and indentation characters as per runbook
        text_str = re.sub(r'[,\t\n\r]', ' ', text_str)
        
        # Remove extra whitespace
        text_str = ' '.join(text_str.split())
        
        return text_str.strip()

    def _clean_date(self, date_value):
        """Clean and standardize dates: YYYY-MM-DD format."""
        if pd.isna(date_value) or not str(date_value).strip():
            return ""
        
        date_str = str(date_value).strip()
        
        # Handle date ranges - take the end date as per runbook
        if ' - ' in date_str or ' to ' in date_str:
            parts = re.split(r' - | to ', date_str)
            if len(parts) >= 2:
                date_str = parts[-1].strip()
        
        # Handle MM/DD/YYYY format (common in Fed data)
        date_match = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})', date_str)
        if date_match:
            month, day, year = date_match.groups()
            try:
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            except:
                pass
        
        # Handle YYYY-MM-DD format (already correct)
        if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
            return date_str
        
        # Handle other date formats
        date_patterns = [
            (r'(\d{4})-(\d{1,2})-(\d{1,2})', lambda m: f"{m.group(1)}-{m.group(2).zfill(2)}-{m.group(3).zfill(2)}"),
            (r'(\d{1,2})-(\d{1,2})-(\d{4})', lambda m: f"{m.group(3)}-{m.group(1).zfill(2)}-{m.group(2).zfill(2)}")
        ]
        
        for pattern, formatter in date_patterns:
            match = re.search(pattern, date_str)
            if match:
                try:
                    return formatter(match)
                except:
                    continue
        
        return date_str

    def _clean_time(self, time_value):
        """Clean and standardize times: HH:MM format."""
        if pd.isna(time_value) or not str(time_value).strip():
            return ""
        
        time_str = str(time_value).strip()
        
        # Handle time ranges - take the start time
        if '-' in time_str and ':' in time_str:
            parts = time_str.split('-')
            if len(parts) >= 1:
                time_str = parts[0].strip()
        
        # Convert 12-hour to 24-hour format and standardize
        time_match = re.search(r'(\d{1,2}):(\d{2})\s*(am|pm)?', time_str.lower())
        if time_match:
            hour, minute, ampm = time_match.groups()
            hour = int(hour)
            
            if ampm:
                if ampm == 'pm' and hour != 12:
                    hour += 12
                elif ampm == 'am' and hour == 12:
                    hour = 0
            
            return f"{hour:02d}:{minute}"
        
        # Handle 24-hour format
        if re.match(r'\d{1,2}:\d{2}', time_str):
            parts = time_str.split(':')
            if len(parts) >= 2:
                try:
                    hour = int(parts[0])
                    minute = int(parts[1])
                    return f"{hour:02d}:{minute:02d}"
                except:
                    pass
        
        return time_str

def determine_release_date_from_data(df: pd.DataFrame) -> str:
    """Determine the Release_Date: biggest date from PERIOD/DATE columns in YYYYMMDD format."""
    
    # Find all date columns
    date_columns = [col for col in df.columns if 'date' in col.lower() or 'period' in col.lower()]
    
    latest_date = None
    
    for col in date_columns:
        for value in df[col].dropna():
            if not value or pd.isna(value):
                continue
                
            value_str = str(value).strip()
            
            # Handle date ranges - take the end date
            if ' - ' in value_str or ' to ' in value_str:
                parts = re.split(r' - | to ', value_str)
                if len(parts) >= 2:
                    value_str = parts[-1].strip()
            
            # Try different date formats and convert to YYYYMMDD
            date_formats = [
                (r'(\d{1,2})/(\d{1,2})/(\d{4})', lambda m: f"{m.group(3)}{m.group(1).zfill(2)}{m.group(2).zfill(2)}"),  # MM/DD/YYYY
                (r'(\d{4})-(\d{1,2})-(\d{1,2})', lambda m: f"{m.group(1)}{m.group(2).zfill(2)}{m.group(3).zfill(2)}"),  # YYYY-MM-DD
                (r'(\d{1,2})-(\d{1,2})-(\d{4})', lambda m: f"{m.group(3)}{m.group(1).zfill(2)}{m.group(2).zfill(2)}"),  # MM-DD-YYYY
            ]
            
            for pattern, formatter in date_formats:
                matches = re.findall(pattern, value_str)
                for match_groups in matches:
                    try:
                        if isinstance(match_groups, tuple) and len(match_groups) == 3:
                            # Create a mock match object for the formatter
                            class MockMatch:
                                def __init__(self, groups):
                                    self.groups = groups
                                def group(self, n):
                                    return self.groups[n-1]
                            
                            mock_match = MockMatch(match_groups)
                            date_str = formatter(mock_match)
                            
                            # Validate the date format
                            if re.match(r'\d{8}', date_str) and len(date_str) == 8:
                                if not latest_date or date_str > latest_date:
                                    latest_date = date_str
                    except Exception as e:
                        continue
    
    # If no date found, use today's date
    if not latest_date:
        latest_date = datetime.now().strftime('%Y%m%d')
    
    return latest_date

@functions_framework.cloud_event
def transform_fedao_csv_ai(cloud_event):
    """Simplified FEDAO CSV transformation function that works with extracted raw data."""
    try:
        gcs_event_data = cloud_event.data
        bucket_name = gcs_event_data["bucket"]
        file_name = gcs_event_data["name"]

        # Only process raw CSV files
        if 'raw_manual_uploads' not in file_name or not file_name.lower().endswith('.csv') or '.keep' in file_name.lower():
            logging.info(f"Ignoring file: {file_name}")
            return {"status": "ignored", "message": "File is not a target raw CSV"}

        customer_id = os.environ.get("CUSTOMER_ID_FOR_FEDAO", "simba")
        project_config_name = "fedao_project"
        active_logger = setup_logging(customer_id, project_config_name)
        active_logger.info(f"Starting transformation for: gs://{bucket_name}/{file_name}")

        customer_config = load_customer_config(customer_id)
        gcp_project_id = customer_config.get("gcp_project_id", os.environ.get("GCP_PROJECT"))

        active_logger.info(f"Using GCP Project: {gcp_project_id}")

        # Initialize the processor
        processor = FEDAODataProcessor()
        
        try:
            db = firestore.Client(project=gcp_project_id)
            project_config = load_dynamic_site_config(db, project_config_name, active_logger)
        except Exception as e:
            active_logger.warning(f"Could not load project config from Firestore: {e}. Using defaults.")
            project_config = create_default_project_config()

        # Determine data type
        raw_filename_base = os.path.basename(file_name)
        if "FEDAO_MOA_RAW_DATA" in raw_filename_base:
            data_type = "MOA"
            config_key_prefix = "FEDAO_MOA_RAW_DATA"
        elif "FEDAO_TOA_RAW_DATA" in raw_filename_base:
            data_type = "TOA"
            config_key_prefix = "FEDAO_TOA_RAW_DATA"
        else:
            raise ValueError(f"Cannot determine data type from filename: {raw_filename_base}")

        active_logger.info(f"Processing as {data_type} data type")

        # Load source data
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

        # Load the raw CSV data
        df = pd.read_csv(StringIO(csv_content_str), dtype=str).fillna('')
        active_logger.info(f"Loaded raw CSV: {len(df)} rows, {len(df.columns)} columns")
        active_logger.info(f"Original columns: {list(df.columns)}")
        
        if len(df) > 0:
            active_logger.info(f"First row sample: {df.iloc[0].to_dict()}")

        # Process the data based on type
        if data_type == "MOA":
            transformed_df = processor.process_moa_data(df)
        else:  # TOA
            transformed_df = processor.process_toa_data(df)
        
        # Determine Release_Date according to runbook
        release_date = determine_release_date_from_data(df)
        transformed_df['Release_Date'] = release_date
        
        active_logger.info(f"Final transformed data: {len(transformed_df)} rows, {len(transformed_df.columns)} columns")
        active_logger.info(f"Release_Date set to: {release_date}")
        active_logger.info(f"Final columns: {list(transformed_df.columns)}")
        
        # Generate output files
        ts_suffix = datetime.now().strftime('%Y%m%d_%H%M%S')
        processed_csv_filename = f"FEDAO_PROCESSED_{config_key_prefix}_{ts_suffix}.csv"
        processed_csv_gcs_path = os.path.join(input_config["gcs_processed_path_root"], datetime.now().strftime('%Y%m%d'), processed_csv_filename)
        
        # Upload processed snapshot
        processed_blob = source_bucket_obj.blob(processed_csv_gcs_path)
        processed_csv_content = transformed_df.to_csv(index=False, quoting=csv.QUOTE_MINIMAL, na_rep='')
        processed_blob.upload_from_string(processed_csv_content, content_type='text/csv')
        active_logger.info(f"Uploaded processed CSV: gs://{bucket_name}/{processed_csv_gcs_path}")
        
        # Update master file
        master_filename = input_config.get("master_filename_template")
        master_file_gcs_path = "Not configured"
        
        if master_filename:
            incremental_master_path = os.path.join(input_config["gcs_incremental_path_root"], master_filename)
            master_blob = source_bucket_obj.blob(incremental_master_path)
            
            active_logger.info(f"Processing master file: {incremental_master_path}")
            
            if master_blob.exists():
                existing_content = master_blob.download_as_text()
                if existing_content.strip():
                    existing_df = pd.read_csv(StringIO(existing_content), dtype=str).fillna('')
                    active_logger.info(f"Existing master file has {len(existing_df)} rows")
                    
                    # Ensure column alignment
                    target_columns = list(transformed_df.columns)
                    existing_df = existing_df.reindex(columns=target_columns, fill_value='')
                    transformed_df = transformed_df.reindex(columns=target_columns, fill_value='')
                    
                    # Combine the data
                    combined_df = pd.concat([existing_df, transformed_df], ignore_index=True)
                else:
                    combined_df = transformed_df
                    active_logger.info("Existing master file was empty, using new data")
            else:
                combined_df = transformed_df
                active_logger.info("No existing master file, creating new")

            # Deduplication based on key columns
            dedup_cols_map = {
                "FEDAO_MOA_RAW_DATA": ['OPERATION DATE', 'OPERATION TYPE'],
                "FEDAO_TOA_RAW_DATA": ['DATE', 'OPERATION TYPE']
            }
            
            available_dedup_cols = [col for col in dedup_cols_map[config_key_prefix] if col in combined_df.columns]
            
            initial_count = len(combined_df)
            if available_dedup_cols:
                # Remove rows where all dedup columns are empty
                non_empty_mask = combined_df[available_dedup_cols].apply(lambda x: x.astype(str).str.strip().ne('').any(), axis=1)
                combined_df = combined_df[non_empty_mask]
                
                # Then deduplicate
                combined_df.drop_duplicates(subset=available_dedup_cols, keep='last', inplace=True)
                active_logger.info(f"Deduplication: {initial_count} -> {len(combined_df)} rows using columns: {available_dedup_cols}")
            else:
                active_logger.warning("No deduplication columns available")
            
            # Upload updated master file
            master_csv_content = combined_df.to_csv(index=False, quoting=csv.QUOTE_MINIMAL, na_rep='')
            master_blob.upload_from_string(master_csv_content, content_type='text/csv')
            master_file_gcs_path = f"gs://{bucket_name}/{incremental_master_path}"
            active_logger.info(f"Updated master file: {master_file_gcs_path} ({len(combined_df)} rows)")

        active_logger.info("Transformation completed successfully")
        return {
            "status": "success",
            "processing_method": "Direct CSV Processing with Minimal Transformations",
            "data_type": data_type,
            "transformed_rows": len(transformed_df),
            "release_date": release_date,
            "processed_snapshot_gcs_path": f"gs://{bucket_name}/{processed_csv_gcs_path}",
            "master_incremental_gcs_path": master_file_gcs_path,
            "final_columns": list(transformed_df.columns)
        }
        
    except Exception as e:
        final_logger = logging.getLogger(__name__)
        final_logger.error(f"Transform function failed: {e}", exc_info=True)
        raise