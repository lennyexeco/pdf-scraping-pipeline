#!/bin/bash

# Enhanced FEDAO Deployment Script
# Configuration variables
PROJECT_ID="execo-simba"
REGION="europe-west1"
ARTIFACT_REGISTRY_REGION="europe-west1"
ARTIFACT_REGISTRY_REPO="pdf-pipeline-services"
SERVICE_ACCOUNT="scraper-service-account@$PROJECT_ID.iam.gserviceaccount.com"
TOP_LEVEL_SRC_DIR="src"
LOG_DIR="logs"
LOG_FILE="$LOG_DIR/deploy_$(date +%Y%m%d_%H%M%S).log"

# FEDAO specific
FEDAO_RAW_CSV_BUCKET_NAME="execo-simba-fedao-poc"

# Service names
WEB_RENDERER_SERVICE_NAME="advanced-web-renderer"
ANALYZE_SCHEMA_FUNCTION_NAME="analyze-website-schema"

# Create log directory
mkdir -p "$LOG_DIR"

# Exit on error
set -e

# --- Helper Functions ---
log_and_echo() {
  echo "$1" | tee -a "$LOG_FILE"
}

check_command() {
  if ! command -v "$1" &> /dev/null; then
    log_and_echo "Error: $1 is required. Please install it."
    exit 1
  fi
}

# Check for required tools
check_command "jq"
check_command "gcloud"

# Function to check requirements.txt
check_requirements() {
  local req_file_path="$1"
  local component_name="$2"
  local actual_req_file

  if [[ "$req_file_path" == temp_deploy_staging* ]]; then
    actual_req_file="$req_file_path"
  elif [ -f "$TOP_LEVEL_SRC_DIR/$req_file_path" ]; then
    actual_req_file="$TOP_LEVEL_SRC_DIR/$req_file_path"
  elif [ -f "$req_file_path" ]; then
    actual_req_file="$req_file_path"
  else
    log_and_echo "Error: requirements.txt path resolution failed for '$req_file_path' in '$component_name'."
    exit 1
  fi

  if [ ! -f "$actual_req_file" ]; then
    log_and_echo "Error: requirements.txt not found at $actual_req_file for $component_name."
    exit 1
  fi
}

# Deploy a single Cloud Function with enhanced error handling
deploy_single_function() {
  local function_name="$1"
  local entry_point_name="$2"
  local trigger_type="$3"
  local trigger_value="$4"
  local function_code_subdir="$5"
  local memory="$6"
  local timeout_seconds="$7"
  local function_log_file="$LOG_DIR/${function_name}_deploy_$(date +%Y%m%d_%H%M%S).log"
  local gcs_event_type=""
  local extra_env_vars="$8"

  if [ "$trigger_type" == "gcs" ]; then
    gcs_event_type="${extra_env_vars}"
    extra_env_vars=""
    if [ -z "$gcs_event_type" ]; then
      log_and_echo "Error: GCS event type not provided for GCS trigger on $function_name."
      exit 1
    fi
  fi

  log_and_echo "Preparing deployment for Cloud Function: $function_name..."
  echo "Deployment log for $function_name: $function_log_file" >> "$LOG_FILE"

  local staging_dir="temp_deploy_staging/$function_name"
  rm -rf "$staging_dir" && mkdir -p "$staging_dir"

  local source_path="$TOP_LEVEL_SRC_DIR/functions/$function_code_subdir"
  if [ ! -d "$source_path" ]; then
    log_and_echo "Error: Function source directory $source_path does not exist."
    exit 1
  fi

  cp -r "$source_path/"* "$staging_dir/"
  if [ ! -f "$staging_dir/main.py" ]; then
    log_and_echo "Error: main.py not found in $staging_dir for $function_name."
    exit 1
  fi

  mkdir -p "$staging_dir/src"
  cp -r "$TOP_LEVEL_SRC_DIR/common" "$staging_dir/src/"
  cp -r "$TOP_LEVEL_SRC_DIR/configs" "$staging_dir/src/"

  check_requirements "$staging_dir/requirements.txt" "$function_name"

  local trigger_options=""
  if [ "$trigger_type" == "topic" ]; then
    trigger_options="--trigger-topic=$trigger_value"
  elif [ "$trigger_type" == "http" ]; then
    trigger_options="--trigger-http --no-allow-unauthenticated"
  elif [ "$trigger_type" == "gcs" ]; then
    trigger_options="--trigger-resource=$trigger_value --trigger-event=$gcs_event_type"
  else
    log_and_echo "Error: Unknown trigger type '$trigger_type' for $function_name."
    exit 1
  fi

  local base_env_vars="GCP_PROJECT=${PROJECT_ID},LOG_LEVEL=INFO,PYTHONUNBUFFERED=1,FUNCTION_REGION=${REGION}"
  local function_specific_env_vars=""
  
  if [[ "$function_name" == "scrape-fedao-sources" ]]; then
    function_specific_env_vars+=",FEDAO_OUTPUT_BUCKET=${FEDAO_RAW_CSV_BUCKET_NAME}"
    function_specific_env_vars+=",FEDAO_TREASURY_URL=https://www.newyorkfed.org/markets/domestic-market-operations/monetary-policy-implementation/treasury-securities/treasury-securities-operational-details"
    function_specific_env_vars+=",FEDAO_AMBS_URL=https://www.newyorkfed.org/markets/ambs_operation_schedule#tabs-2"
  elif [[ "$function_name" == "transform-fedao-csv" ]]; then
    function_specific_env_vars+=",CUSTOMER_ID_FOR_FEDAO=simba"
  fi

  local final_env_vars="$base_env_vars$function_specific_env_vars"
  if [ -n "$extra_env_vars" ] && [[ ! "$extra_env_vars" =~ "google.storage" ]]; then
    final_env_vars+=",$extra_env_vars"
  fi

  log_and_echo "Deploying $function_name with ENV: $final_env_vars"
  if ! gcloud functions deploy "$function_name" \
    --runtime=python39 \
    $trigger_options \
    --source="$staging_dir" \
    --entry-point="$entry_point_name" \
    --project "$PROJECT_ID" \
    --region "$REGION" \
    --service-account "$SERVICE_ACCOUNT" \
    --timeout "${timeout_seconds}s" \
    --memory "$memory" \
    --gen2 \
    --set-env-vars "$final_env_vars" \
    >> "$function_log_file" 2>&1; then
    log_and_echo "Error: Deployment of $function_name failed. Check $function_log_file."
    return 1
  fi

  log_and_echo "$function_name deployed successfully."
  rm -rf "$staging_dir"
  return 0
}

# --- Main Script ---
log_and_echo "Starting Enhanced FEDAO deployment process at $(date)"

log_and_echo "Verifying gcloud authentication and project settings..."
if ! gcloud config set project "$PROJECT_ID" >> "$LOG_FILE" 2>&1; then
  log_and_echo "Error: Failed to set project $PROJECT_ID. Check gcloud authentication."
  exit 1
fi

log_and_echo "Enabling required Google Cloud services..."
gcloud services enable run.googleapis.com \
                       artifactregistry.googleapis.com \
                       cloudbuild.googleapis.com \
                       cloudfunctions.googleapis.com \
                       pubsub.googleapis.com \
                       firestore.googleapis.com \
                       iam.googleapis.com \
                       aiplatform.googleapis.com \
                       eventarc.googleapis.com \
                       --project="$PROJECT_ID" >> "$LOG_FILE" 2>&1
log_and_echo "Required Google Cloud services enabled."

log_and_echo "Granting Vertex AI User role to service account..."
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$SERVICE_ACCOUNT" \
  --role="roles/aiplatform.user" \
  --condition=None >> "$LOG_FILE" 2>&1
log_and_echo "Service account permissions updated."


# Deploy functions
DEPLOY_SUCCESS_COUNT=0
DEPLOY_FAIL_COUNT=0

log_and_echo "Deploying Cloud Functions..."

# Deploy transform function
log_and_echo "Deploying transform-fedao-csv function..."
if deploy_single_function "transform-fedao-csv" "transform_fedao_csv_ai" \
  "gcs" "$FEDAO_RAW_CSV_BUCKET_NAME" \
  "transform_fedao_csv" "1Gi" "540" \
  "google.storage.object.finalize"; then
  ((DEPLOY_SUCCESS_COUNT++))
  log_and_echo "✓ transform-fedao-csv deployed successfully"
else
  ((DEPLOY_FAIL_COUNT++))
  log_and_echo "✗ transform-fedao-csv deployment failed"
fi

# Deploy scraper function
log_and_echo "Deploying scrape-fedao-sources function..."
if deploy_single_function "scrape-fedao-sources" "scrape_fedao_sources_ai" \
  "topic" "scrape-fedao-sources-topic" \
  "scrape_fedao_sources" "2Gi" "540" \
  ""; then
  ((DEPLOY_SUCCESS_COUNT++))
  log_and_echo "✓ scrape-fedao-sources deployed successfully"
else
  ((DEPLOY_FAIL_COUNT++))
  log_and_echo "✗ scrape-fedao-sources deployment failed"
fi

# Summary
log_and_echo "-------------------------------------------------"
log_and_echo "FEDAO Deployment Summary:"
log_and_echo "Successful deployments: $DEPLOY_SUCCESS_COUNT"
log_and_echo "Failed deployments:     $DEPLOY_FAIL_COUNT"
log_and_echo "GCS Bucket: gs://$FEDAO_RAW_CSV_BUCKET_NAME"
log_and_echo "-------------------------------------------------"

if [ "$DEPLOY_FAIL_COUNT" -gt 0 ]; then
  log_and_echo "⚠️  Some deployments failed. Check logs in $LOG_DIR"
  exit 1
fi

log_and_echo "✅ All deployments completed successfully!"
log_and_echo ""
log_and_echo "🔧 Testing Instructions:"
log_and_echo "1. Test the scraper by publishing a message to its Pub/Sub topic:"
log_and_echo "   gcloud pubsub topics publish scrape-fedao-sources-topic --message='{}' --project=$PROJECT_ID"
log_and_echo ""
log_and_echo "2. Monitor function logs in the Google Cloud Console or via the CLI:"
log_and_echo "   gcloud functions logs read scrape-fedao-sources --gen2 --project=$PROJECT_ID --region=$REGION --limit=50"
log_and_echo "   gcloud functions logs read transform-fedao-csv --gen2 --project=$PROJECT_ID --region=$REGION --limit=50"
log_and_echo ""
log_and_echo "3. Check GCS bucket for the output files after the pipeline runs:"
log_and_echo "   gsutil ls -r gs://$FEDAO_RAW_CSV_BUCKET_NAME/FEDAO/"
log_and_echo ""
log_and_echo "Deployment completed at $(date)"