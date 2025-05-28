#!/bin/bash

# Configuration variables
PROJECT_ID="execo-harvey" # As per your config
REGION="europe-west1"
SERVICE_ACCOUNT="scraper-service-account@$PROJECT_ID.iam.gserviceaccount.com"
TOP_LEVEL_SRC_DIR="src"
LOG_DIR="logs"
LOG_FILE="$LOG_DIR/deploy_$(date +%Y%m%d_%H%M%S).log"

# Create log directory
mkdir -p "$LOG_DIR"

# Exit on error
set -e

# Check quota project in ADC
check_quota_project() {
  echo "Checking Application Default Credentials quota project..." | tee -a "$LOG_FILE"
  ADC_FILE="$HOME/.config/gcloud/application_default_credentials.json"
  if [ ! -f "$ADC_FILE" ]; then
    echo "Error: ADC file $ADC_FILE not found. Run 'gcloud auth application-default login'." | tee -a "$LOG_FILE"
    exit 1
  fi
  QUOTA_PROJECT=$(grep -o '"quota_project_id": "[^"]*"' "$ADC_FILE" | cut -d'"' -f4)
  if [ "$QUOTA_PROJECT" != "$PROJECT_ID" ]; then
    echo "Error: ADC quota project ($QUOTA_PROJECT) does not match active project ($PROJECT_ID)." | tee -a "$LOG_FILE"
    echo "Run 'gcloud auth application-default set-quota-project $PROJECT_ID' to fix." | tee -a "$LOG_FILE"
    exit 1
  fi
  echo "Quota project verified: $QUOTA_PROJECT" | tee -a "$LOG_FILE"
}

# Check requirements.txt for critical dependencies
check_requirements() {
  local req_file="$1"
  local function_name="$2"
  # Check if running in staging dir context or original source
  local actual_req_file
  if [[ "$req_file" == temp_deploy_staging* ]]; then
    actual_req_file="$req_file" # Already in staging
  elif [ -f "$TOP_LEVEL_SRC_DIR/$req_file/requirements.txt" ]; then # Path is function_code_subdir
     actual_req_file="$TOP_LEVEL_SRC_DIR/$req_file/requirements.txt"
  elif [ -f "$req_file" ]; then # Path is direct to requirements.txt
     actual_req_file="$req_file"
  else
    echo "Error: requirements.txt path resolution failed for '$req_file' in function '$function_name'." | tee -a "$LOG_FILE"
    exit 1
  fi

  if [ -f "$actual_req_file" ]; then
    if ! grep -q "google-cloud-secret-manager" "$actual_req_file"; then
      echo "Warning: google-cloud-secret-manager missing in $actual_req_file for $function_name. Ensure it's included if secrets are used." | tee -a "$LOG_FILE"
      # echo "google-cloud-secret-manager==2.20.2" >> "$actual_req_file" # Example of adding
    fi
    if ! grep -q "google-cloud-logging" "$actual_req_file"; then
      echo "Warning: google-cloud-logging missing in $actual_req_file for $function_name. Ensure it's included for custom logging." | tee -a "$LOG_FILE"
      # echo "google-cloud-logging==3.10.0" >> "$actual_req_file"
    fi
    if [ "$function_name" = "retry-pipeline" ] || [ "$function_name" = "discover-main-urls" ] || [ "$function_name" = "extract-initial-metadata" ] && ! grep -q "google-cloud-aiplatform" "$actual_req_file"; then
      echo "Warning: google-cloud-aiplatform missing in $actual_req_file for $function_name (which uses Vertex AI). Ensure it's included." | tee -a "$LOG_FILE"
      # echo "google-cloud-aiplatform==1.40.0" >> "$actual_req_file"
    fi
     # Add requests and beautifulsoup4 for scraping functions
    if [[ "$function_name" == *"discover-main-urls"* ]] || [[ "$function_name" == *"extract-initial-metadata"* ]] || [[ "$function_name" == *"fetch-content"* ]]; then
      if ! grep -q "requests" "$actual_req_file"; then
        echo "Warning: 'requests' missing in $actual_req_file for scraping function $function_name. Ensure it's included." | tee -a "$LOG_FILE"
      fi
      if ! grep -q "beautifulsoup4" "$actual_req_file"; then
        echo "Warning: 'beautifulsoup4' missing in $actual_req_file for scraping function $function_name. Ensure it's included." | tee -a "$LOG_FILE"
      fi
    fi
  else
    echo "Error: requirements.txt not found at $actual_req_file for $function_name." | tee -a "$LOG_FILE"
    exit 1
  fi
}

# Helper function for deployment
deploy_single_function() {
  local function_name="$1"
  local entry_point_name="$2"
  local trigger_type="$3" # "topic", "http", "gcs"
  local trigger_value="$4" # Topic name, GCS bucket, or "N/A" for HTTP
  local function_code_subdir="$5"
  local memory="$6"
  local timeout="$7"
  local function_log_file="$LOG_DIR/${function_name}_$(date +%Y%m%d_%H%M%S).log"
  local gcs_event_type="" # For GCS triggers

  if [ "$trigger_type" == "gcs" ]; then
    gcs_event_type="$8" # e.g. google.storage.object.finalize
    if [ -z "$gcs_event_type" ]; then
      echo "Error: GCS event type not provided for GCS trigger on $function_name." | tee -a "$LOG_FILE"
      exit 1
    fi
  fi


  echo "Preparing deployment for $function_name..." | tee -a "$LOG_FILE"
  echo "Preparing deployment for $function_name..." >> "$function_log_file"

  # Normalize paths to avoid double slashes
  function_code_subdir=$(echo "$function_code_subdir" | sed 's|//|/|g')
  local staging_dir="temp_deploy_staging/$function_name"

  # Clean up any existing staging directory
  if [ -d "$staging_dir" ]; then
    rm -rf "$staging_dir"
  fi
  mkdir -p "$staging_dir"

  # Verify function source directory exists
  if [ ! -d "$TOP_LEVEL_SRC_DIR/$function_code_subdir" ]; then
    echo "Error: Function source directory $TOP_LEVEL_SRC_DIR/$function_code_subdir does not exist." | tee -a "$LOG_FILE" "$function_log_file"
    exit 1
  fi

  # Copy function-specific code
  cp -R "$TOP_LEVEL_SRC_DIR/$function_code_subdir/"* "$staging_dir/"

  # Verify main.py was copied
  if [ ! -f "$staging_dir/main.py" ]; then
    echo "Error: main.py not found in $staging_dir after copying from $TOP_LEVEL_SRC_DIR/$function_code_subdir." | tee -a "$LOG_FILE" "$function_log_file"
    exit 1
  fi

  # Copy shared 'common' and 'configs' directories to staging_dir/src/
  mkdir -p "$staging_dir/src"
  if [ -d "$TOP_LEVEL_SRC_DIR/common" ]; then
    cp -R "$TOP_LEVEL_SRC_DIR/common" "$staging_dir/src/"
  else
    echo "Warning: $TOP_LEVEL_SRC_DIR/common not found, skipping." | tee -a "$LOG_FILE" "$function_log_file"
  fi
  if [ -d "$TOP_LEVEL_SRC_DIR/configs" ]; then
    cp -R "$TOP_LEVEL_SRC_DIR/configs" "$staging_dir/src/"
  else
    echo "Warning: $TOP_LEVEL_SRC_DIR/configs not found, skipping." | tee -a "$LOG_FILE" "$function_log_file"
  fi

  # Copy and check requirements.txt
  local req_file_source_path="$TOP_LEVEL_SRC_DIR/$function_code_subdir/requirements.txt"
  local req_file_staging_path="$staging_dir/requirements.txt"
  if [ -f "$req_file_source_path" ]; then
    cp "$req_file_source_path" "$req_file_staging_path"
    check_requirements "$req_file_staging_path" "$function_name" # Pass staging path to check_requirements
  else
    echo "Error: requirements.txt not found in $req_file_source_path for $function_name." | tee -a "$LOG_FILE" "$function_log_file"
    exit 1
  fi

  local trigger_options=""
  if [ "$trigger_type" == "topic" ]; then
    trigger_options="--trigger-topic=$trigger_value"
  elif [ "$trigger_type" == "http" ]; then
    trigger_options="--trigger-http --no-allow-unauthenticated" # Secure by default
  elif [ "$trigger_type" == "gcs" ]; then
    trigger_options="--trigger-resource=$trigger_value --trigger-event=$gcs_event_type"
  else
    echo "Error: Unknown trigger type '$trigger_type' for $function_name." | tee -a "$LOG_FILE" "$function_log_file"
    exit 1
  fi

  echo "Deploying $function_name..." | tee -a "$LOG_FILE"
  echo "Deploying $function_name..." >> "$function_log_file"
  if ! gcloud functions deploy "$function_name" \
    --runtime=python39 \
    $trigger_options \
    --source="$staging_dir" \
    --entry-point="$entry_point_name" \
    --project="$PROJECT_ID" \
    --region="$REGION" \
    --service-account="$SERVICE_ACCOUNT" \
    --timeout="${timeout}s" \
    --memory="$memory" \
    --no-gen2 \
    >> "$function_log_file" 2>&1; then
    echo "Error: Deployment of $function_name failed. Check $function_log_file for details." | tee -a "$LOG_FILE"
    cat "$function_log_file" >> "$LOG_FILE" # Append specific function log to main log
    # Do not exit here, let the script try to deploy other functions or handle errors at the end.
    # Consider adding a failure flag and exiting at the end if any function failed.
    return 1 # Indicate failure
  fi

  echo "$function_name deployed successfully." | tee -a "$LOG_FILE"
  echo "$function_name deployed successfully." >> "$function_log_file"

  # Clean up staging directory
  rm -rf "$staging_dir"
  echo "Cleaned up staging directory for $function_name." | tee -a "$LOG_FILE"
  echo "Cleaned up staging directory for $function_name." >> "$function_log_file"
  return 0 # Indicate success
}

# Verify gcloud authentication
echo "Verifying gcloud authentication..." | tee -a "$LOG_FILE"
if ! gcloud config set project "$PROJECT_ID" >> "$LOG_FILE" 2>&1; then
  echo "Error: Failed to set project $PROJECT_ID." | tee -a "$LOG_FILE"
  exit 1
fi

# Check quota project
check_quota_project

# Define Pub/Sub topics for the new workflow
# Note: process-data, fix-image-urls, store-html topics from old script might be deprecated by this new flow.
echo "Checking for Pub/Sub topics..." | tee -a "$LOG_FILE"
topics_to_ensure=(
  "start-ingest-category-urls-topic"    # For ingest-category-urls
  "start-analyze-website-schema-topic"  # For analyze-website-schema
  "discover-main-urls-topic"          # For discover-main-urls
  "extract-initial-metadata-topic"    # For extract-initial-metadata
  "fetch-content-topic"               # For fetch-content
  "generate-xml-topic"                # For generate-xml (was "generate-xml")
  "generate-reports-topic"            # For generate-reports (was "generate-reports")
  "retry-pipeline"                    # For retry-pipeline (existing)
)

for topic in "${topics_to_ensure[@]}"; do
  if ! gcloud pubsub topics describe "$topic" --project="$PROJECT_ID" >> "$LOG_FILE" 2>&1; then
    echo "Creating $topic topic..." | tee -a "$LOG_FILE"
    gcloud pubsub topics create "$topic" --project="$PROJECT_ID" >> "$LOG_FILE" 2>&1
  else
    echo "Topic $topic already exists." | tee -a "$LOG_FILE"
  fi
done

# --- Deploy Cloud Functions for the New Workflow ---
# Phase 1: URL Ingestion & Discovery
# ingest-category-urls: Triggered by GCS for CSV uploads, or HTTP for manual start
# For this script, let's use a Pub/Sub trigger for consistency, assuming another process (manual or GCS trigger -> Pub/Sub) starts it.
# Or, could be an HTTP trigger if preferred for manual/external invocation.
# If GCS triggered: deploy_single_function "ingest-category-urls" "ingest_category_urls" "gcs" "your-input-bucket-name" "functions/ingest_category_urls" "256MB" "300" "google.storage.object.finalize"
# deploy_single_function "ingest-category-urls" "ingest_category_urls" "topic" "start-ingest-category-urls-topic" "functions/ingest_category_urls" "256MB" "300"
# deploy_single_function "analyze-website-schema" "analyze_website_schema" "topic" "start-analyze-website-schema-topic" "functions/analyze_website_schema" "1GB" "540"
# deploy_single_function "discover-main-urls" "discover_main_urls" "topic" "discover-main-urls-topic" "functions/discover_main_urls" "1GB" "540" # Potentially needs more memory/timeout for scraping

# Phase 2: Metadata & Content Ingestion
# deploy_single_function "extract-initial-metadata" "extract_initial_metadata" "topic" "extract-initial-metadata-topic" "functions/extract_initial_metadata" "1GB" "540" # Potentially needs more memory for scraping & AI
deploy_single_function "fetch-content" "fetch_content" "topic" "fetch-content-topic" "functions/fetch_content" "512MB" "540"

# Phase 3: XML Generation & Reporting (These were in your original script)
deploy_single_function "generate-xml" "generate_xml" "topic" "generate-xml-topic" "functions/generate_xml" "512MB" "540"
deploy_single_function "generate-reports" "generate_reports" "topic" "generate-reports-topic" "functions/generate_reports" "512MB" "540"

# Utility Functions (This was in your original script)
deploy_single_function "retry-pipeline" "retry_pipeline" "topic" "retry-pipeline" "functions/retry_pipeline" "1GB" "300" # AI analysis might need more memory

echo "All specified Cloud Functions deployment attempts are complete!" | tee -a "$LOG_FILE"
echo "Please check individual logs in the '$LOG_DIR' directory and the main log '$LOG_FILE' for status of each deployment." | tee -a "$LOG_FILE"
