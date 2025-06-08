#!/bin/bash

# FEDAO Configuration Setup Script
PROJECT_ID="execo-simba"
CONFIG_FILE="fedao_project_config.json"

echo "Setting up FEDAO configuration in Firestore..."

# Create the project config JSON file
cat > "$CONFIG_FILE" << 'EOF'
{
  "project_name": "fedao_project",
  "project_abbreviation": "FEDAO",
  "customer_id": "simba",
  "description": "Federal Reserve Bank of New York - Fed Announced Operations",
  
  "firestore_collection": "fedao_documents",
  "firestore_counters_collection": "fedao_counters",
  "pipeline_version_tag": "1.0.0",
  
  "sequential_id_config": {
    "enabled": true,
    "firestore_counters_collection": "fedao_counters",
    "counter_doc_prefix": "FEDAO_id_sequence"
  },
  
  "fedao_input_configs": {
    "FEDAO_MOA_RAW_DATA": {
      "transformations": [
        {
          "type": "add_release_date",
          "date_source_column": "PERIOD"
        },
        {
          "type": "split_max_operation_size"
        }
      ],
      "gcs_processed_path_template": "FEDAO/processed_csvs/{date_str}/FEDAO_MOA_PROCESSED_{batch_id}.csv",
      "gcs_incremental_path_template": "FEDAO/master_data/FEDAO_MOA_DATA.csv",
      "output_document_type": "FEDAO_MOA_ITEM"
    },
    "FEDAO_TOA_RAW_DATA": {
      "transformations": [
        {
          "type": "add_release_date",
          "date_source_column": "DATE"
        },
        {
          "type": "split_rows_cusip_max"
        }
      ],
      "gcs_processed_path_template": "FEDAO/processed_csvs/{date_str}/FEDAO_TOA_PROCESSED_{batch_id}.csv",
      "gcs_incremental_path_template": "FEDAO/master_data/FEDAO_TOA_DATA.csv",
      "output_document_type": "FEDAO_TOA_ITEM"
    }
  },
  
  "field_mappings": {
    "Operation_Type": {"source": "OPERATION TYPE"},
    "Security_Type": {"source": "SECURITY TYPE AND MATURITY"},
    "Period": {"source": "PERIOD || DATE"},
    "Maximum_Operation_Currency": {"source": "MAXIMUM_OPERATION_CURRENCY"},
    "Maximum_Operation_Size": {"source": "MAXIMUM_OPERATION_SIZE_VALUE"},
    "Maximum_Operation_Multiplier": {"source": "MAXIMUM_OPERATION_MULTIPLIER"},
    "CUSIP": {"source": "Securities Included (CUSIP)"},
    "Security_Maximum": {"source": "Security Maximums (Millions)"}
  },
  
  "fedao_config": {
    "title_source_columns": ["OPERATION TYPE", "SECURITY TYPE AND MATURITY"],
    "default_topics": ["Federal Reserve Operations", "Treasury Securities", "Monetary Policy"],
    "default_legislation": ["Federal Reserve Act"]
  },
  
  "pubsub_topics": {
    "extract_initial_metadata_topic": "extract-initial-metadata-topic"
  },
  
  "ai_metadata_extraction": {
    "enabled": false
  }
}
EOF

# Upload to Firestore
echo "Uploading project configuration to Firestore..."
python3 << EOF
import json
from google.cloud import firestore

# Load the config
with open('$CONFIG_FILE', 'r') as f:
    config = json.load(f)

# Initialize Firestore with default database
db = firestore.Client(project='$PROJECT_ID', database='(default)')

# Upload to site_configs collection
doc_ref = db.collection('site_configs').document('fedao_project')
doc_ref.set(config)

print("✅ Configuration uploaded successfully to Firestore!")
print("Collection: site_configs")
print("Document: fedao_project")
EOF

# Clean up
rm "$CONFIG_FILE"
echo "✅ FEDAO configuration setup complete!"