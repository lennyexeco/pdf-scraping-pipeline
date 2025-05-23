#!/bin/bash

# Configuration variables
PROJECT_ID="execo-harvey"
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
  if [ -f "$req_file" ]; then
    if ! grep -q "google-cloud-secret-manager" "$req_file"; then
      echo "Warning: google-cloud-secret-manager missing in $req_file for $function_name. Adding it." | tee -a "$LOG_FILE"
      echo "google-cloud-secret-manager==2.20.2" >> "$req_file"
    fi
    if ! grep -q "google-cloud-logging" "$req_file"; then
      echo "Warning: google-cloud-logging missing in $req_file for $function_name. Adding it." | tee -a "$LOG_FILE"
      echo "google-cloud-logging==3.10.0" >> "$req_file"
    fi
    if [ "$function_name" = "retry-pipeline" ] && ! grep -q "google-cloud-aiplatform" "$req_file"; then
      echo "Warning: google-cloud-aiplatform missing in $req_file for $function_name. Adding it." | tee -a "$LOG_FILE"
      echo "google-cloud-aiplatform==1.40.0" >> "$req_file"
    fi
  else
    echo "Error: requirements.txt not found in $req_file for $function_name." | tee -a "$LOG_FILE"
    exit 1
  fi
}

# Helper function for deployment
deploy_single_function() {
  local function_name="$1"
  local entry_point_name="$2"
  local trigger_topic_name="$3"
  local function_code_subdir="$4"
  local memory="$5"
  local timeout="$6"
  local function_log_file="$LOG_DIR/${function_name}_$(date +%Y%m%d_%H%M%S).log"

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
    echo "Error: main.py not found in $staging_dir after copying." | tee -a "$LOG_FILE" "$function_log_file"
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
  local req_file="$TOP_LEVEL_SRC_DIR/$function_code_subdir/requirements.txt"
  if [ -f "$req_file" ]; then
    cp "$req_file" "$staging_dir/"
    check_requirements "$req_file" "$function_name"
  else
    echo "Error: requirements.txt not found in $req_file for $function_name." | tee -a "$LOG_FILE" "$function_log_file"
    exit 1
  fi

  echo "Deploying $function_name..." | tee -a "$LOG_FILE"
  echo "Deploying $function_name..." >> "$function_log_file"
  if ! gcloud functions deploy "$function_name" \
    --runtime=python39 \
    --trigger-topic="$trigger_topic_name" \
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
    cat "$function_log_file" >> "$LOG_FILE"
    exit 1
  fi

  echo "$function_name deployed successfully." | tee -a "$LOG_FILE"
  echo "$function_name deployed successfully." >> "$function_log_file"

  # Clean up staging directory
  rm -rf "$staging_dir"
  echo "Cleaned up staging directory for $function_name." | tee -a "$LOG_FILE"
  echo "Cleaned up staging directory for $function_name." >> "$function_log_file"
}

# Verify gcloud authentication
echo "Verifying gcloud authentication..." | tee -a "$LOG_FILE"
if ! gcloud config set project "$PROJECT_ID" >> "$LOG_FILE" 2>&1; then
  echo "Error: Failed to set project $PROJECT_ID." | tee -a "$LOG_FILE"
  exit 1
fi

# Check quota project
check_quota_project

# Create necessary Pub/Sub topics if they don't exist
echo "Checking for Pub/Sub topics..." | tee -a "$LOG_FILE"
for topic in process-data store-html fix-image-urls generate-xml generate-reports retry-pipeline; do
  if ! gcloud pubsub topics describe "$topic" --project="$PROJECT_ID" >> "$LOG_FILE" 2>&1; then
    echo "Creating $topic topic..." | tee -a "$LOG_FILE"
    gcloud pubsub topics create "$topic" --project="$PROJECT_ID" >> "$LOG_FILE" 2>&1
  fi
done

# Deploy Cloud Functions
# deploy_single_function "extract-metadata" "extract_metadata" "process-data" "functions/extract_metadata" "512MB" "540"
# deploy_single_function "store-html" "store_html" "store-html" "functions/store_html" "512MB" "540"
# deploy_single_function "fix-image-urls" "fix_image_urls" "fix-image-urls" "functions/fix_image_urls" "512MB" "540"
# deploy_single_function "generate-xml" "generate_xml" "generate-xml" "functions/generate_xml" "512MB" "540"
# deploy_single_function "generate-reports" "generate_reports" "generate-reports" "functions/generate_reports" "512MB" "540"
deploy_single_function "retry-pipeline" "retry_pipeline" "retry-pipeline" "functions/retry_pipeline" "1GB" "300"

echo "All Cloud Functions deployed successfully!" | tee -a "$LOG_FILE"