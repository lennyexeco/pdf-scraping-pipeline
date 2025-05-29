#!/bin/bash

# Configuration variables
PROJECT_ID="execo-harvey"
REGION="europe-west1"
SERVICE_ACCOUNT="scraper-service-account@$PROJECT_ID.iam.gserviceaccount.com"
TOP_LEVEL_SRC_DIR="src"
LOG_DIR="logs"
LOG_FILE="$LOG_DIR/deploy_$(date +%Y%m%d_%H%M%S).log"
PROJECT_CONFIGS=(
  "$TOP_LEVEL_SRC_DIR/configs/projects/germany_federal_law.json"
  "$TOP_LEVEL_SRC_DIR/configs/projects/asic_downloads.json"
)

# Create log directory
mkdir -p "$LOG_DIR"

# Exit on error
set -e

# Check for jq
if ! command -v jq &> /dev/null; then
  echo "Error: jq is required. Install it with 'brew install jq' (macOS) or 'sudo apt-get install jq' (Ubuntu)." | tee -a "$LOG_FILE"
  exit 1
fi

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
  local actual_req_file
  if [[ "$req_file" == temp_deploy_staging* ]]; then
    actual_req_file="$req_file"
  elif [ -f "$TOP_LEVEL_SRC_DIR/$req_file/requirements.txt" ]; then
    actual_req_file="$TOP_LEVEL_SRC_DIR/$req_file/requirements.txt"
  elif [ -f "$req_file" ]; then
    actual_req_file="$req_file"
  else
    echo "Error: requirements.txt path resolution failed for '$req_file' in function '$function_name'." | tee -a "$LOG_FILE"
    exit 1
  fi

  if [ -f "$actual_req_file" ]; then
    if ! grep -q "google-cloud-secret-manager" "$actual_req_file"; then
      echo "Warning: google-cloud-secret-manager missing in $actual_req_file for $function_name." | tee -a "$LOG_FILE"
    fi
    if ! grep -q "google-cloud-logging" "$actual_req_file"; then
      echo "Warning: google-cloud-logging missing in $actual_req_file for $function_name." | tee -a "$LOG_FILE"
    fi
    if [ "$function_name" = "retry-pipeline" ] || [ "$function_name" = "discover-main-urls" ] || [ "$function_name" = "extract-initial-metadata" ] && ! grep -q "google-cloud-aiplatform" "$actual_req_file"; then
      echo "Warning: google-cloud-aiplatform missing in $actual_req_file for $function_name." | tee -a "$LOG_FILE"
    fi
    if [[ "$function_name" == *"discover-main-urls"* ]] || [[ "$function_name" == *"extract-initial-metadata"* ]] || [[ "$function_name" == *"fetch-content"* ]]; then
      if ! grep -q "requests" "$actual_req_file"; then
        echo "Warning: 'requests' missing in $actual_req_file for scraping function $function_name." | tee -a "$LOG_FILE"
      fi
      if ! grep -q "beautifulsoup4" "$actual_req_file"; then
        echo "Warning: 'beautifulsoup4' missing in $actual_req_file for scraping function $function_name." | tee -a "$LOG_FILE"
      fi
    fi
  else
    echo "Error: requirements.txt not found at $actual_req_file for $function_name." | tee -a "$LOG_FILE"
    exit 1
  fi
}

# Deploy a single Cloud Function
deploy_single_function() {
  local function_name="$1"
  local entry_point_name="$2"
  local trigger_type="$3"
  local trigger_value="$4"
  local function_code_subdir="$5"
  local memory="$6"
  local timeout="$7"
  local function_log_file="$LOG_DIR/${function_name}_$(date +%Y%m%d_%H%M%S).log"
  local gcs_event_type=""

  if [ "$trigger_type" == "gcs" ]; then
    gcs_event_type="$8"
    if [ -z "$gcs_event_type" ]; then
      echo "Error: GCS event type not provided for GCS trigger on $function_name." | tee -a "$LOG_FILE"
      exit 1
    fi
  fi

  echo "Preparing deployment for $function_name..." | tee -a "$LOG_FILE"
  echo "Preparing deployment for $function_name..." >> "$function_log_file"

  function_code_subdir=$(echo "$function_code_subdir" | sed 's|//|/|g')
  local staging_dir="temp_deploy_staging/$function_name"

  if [ -d "$staging_dir" ]; then
    rm -rf "$staging_dir"
  fi
  mkdir -p "$staging_dir"

  if [ ! -d "$TOP_LEVEL_SRC_DIR/$function_code_subdir" ]; then
    echo "Error: Function source directory $TOP_LEVEL_SRC_DIR/$function_code_subdir does not exist." | tee -a "$LOG_FILE" "$function_log_file"
    exit 1
  fi

  cp -r "$TOP_LEVEL_SRC_DIR/$function_code_subdir/"* "$staging_dir/"

  if [ ! -f "$staging_dir/main.py" ]; then
    echo "Error: main.py not found in $staging_dir after copying." | tee -a "$LOG_FILE" "$function_log_file"
    exit 1
  fi

  mkdir -p "$staging_dir/src"
  if [ -d "$TOP_LEVEL_SRC_DIR/common" ]; then
    cp -r "$TOP_LEVEL_SRC_DIR/common" "$staging_dir/src/"
  fi
  if [ -d "$TOP_LEVEL_SRC_DIR/configs" ]; then
    cp -r "$TOP_LEVEL_SRC_DIR/configs" "$staging_dir/src/"
  fi

  local req_file_source_path="$TOP_LEVEL_SRC_DIR/$function_code_subdir/requirements.txt"
  local req_file_staging_path="$staging_dir/requirements.txt"
  if [ -f "$req_file_source_path" ]; then
    cp "$req_file_source_path" "$req_file_staging_path"
    check_requirements "$req_file_staging_path" "$function_name"
  else
    echo "Error: requirements.txt not found in $req_file_source_path." | tee -a "$LOG_FILE" "$function_log_file"
    exit 1
  fi

  local trigger_options=""
  if [ "$trigger_type" == "topic" ]; then
    trigger_options="--trigger-topic=$trigger_value"
  elif [ "$trigger_type" == "http" ]; then
    trigger_options="--trigger-http --no-allow-unauthenticated"
  elif [ "$trigger_type" == "gcs" ]; then
    trigger_options="--trigger-resource=$trigger_value --trigger-event=$gcs_event_type"
  else
    echo "Error: Unknown trigger type '$function_type' for $function_name." | tee -a "$LOG_FILE" "$function_log_file"
    exit 1
  fi

  echo "Deploying $function_name..." | tee -a "$LOG_FILE"
  if ! gcloud functions deploy "$function_name" \
    --runtime=python39 \
    $trigger_options \
    --source="$staging_dir" \
    --entry-point="$entry_point_name" \
    --project "$PROJECT_ID" \
    --region "$REGION" \
    --service-account "$SERVICE_ACCOUNT" \
    --timeout "${timeout}s" \
    --memory "$memory" \
    --no-gen2 \
    >> "$function_log_file" 2>&1; then
    echo "Error: Deployment of $function_name failed. Check $function_log_file." | tee -a "$LOG_FILE"
    return 1
  fi

  echo "$function_name deployed successfully." | tee -a "$LOG_FILE"
  rm -rf "$staging_dir"
  return 0
}

# Deploy web-renderer Cloud Run service
deploy_web_renderer() {
  local service_name="web-renderer"
  local service_dir="services/web_renderer"
  local image_name="europe-west1-docker.pkg.dev/$PROJECT_ID/web-renderer-repo/$service_name:latest"
  local service_log_file="$LOG_DIR/${service_name}_$(date +%Y%m%d_%H%M%S).log"

  echo "Preparing deployment for $service_name Cloud Run service..." | tee -a "$LOG_FILE"
  echo "Preparing deployment for $service_name..." >> "$service_log_file"

  # Verify service directory exists
  if [ ! -d "$TOP_LEVEL_SRC_DIR/$service_dir" ]; then
    echo "Error: Service source directory $TOP_LEVEL_SRC_DIR/$service_dir does not exist." | tee -a "$LOG_FILE" "$service_log_file"
    exit 1
  fi

  # Verify Dockerfile exists
  if [ ! -f "$TOP_LEVEL_SRC_DIR/$service_dir/Dockerfile" ]; then
    echo "Error: Dockerfile not found in $TOP_LEVEL_SRC_DIR/$service_dir." | tee -a "$LOG_FILE" "$service_log_file"
    exit 1
  fi

  # Verify requirements.txt exists
  if [ ! -f "$TOP_LEVEL_SRC_DIR/$service_dir/requirements.txt" ]; then
    echo "Error: requirements.txt not found in $TOP_LEVEL_SRC_DIR/$service_dir." | tee -a "$LOG_FILE" "$service_log_file"
    exit 1
  fi

  # Build and push Docker image
  echo "Building and pushing Docker image for $service_name..." | tee -a "$LOG_FILE"
  if ! gcloud builds submit "$TOP_LEVEL_SRC_DIR/$service_dir" \
    --tag "$image_name" \
    --project "$PROJECT_ID" \
    --region "$REGION" \
    >> "$service_log_file" 2>&1; then
    echo "Error: Failed to build Docker image for $service_name. Check $service_log_file." | tee -a "$LOG_FILE"
    exit 1
  fi

  # Deploy to Cloud Run
  echo "Deploying $service_name to Cloud Run..." | tee -a "$LOG_FILE"
  if ! gcloud run deploy "$service_name" \
    --image "$image_name" \
    --platform managed \
    --region "$REGION" \
    --no-allow-unauthenticated \
    --memory 2Gi \
    --cpu 2 \
    --timeout 300 \
    --concurrency 80 \
    --project "$PROJECT_ID" \
    --service-account "$SERVICE_ACCOUNT" \
    >> "$service_log_file" 2>&1; then
    echo "Error: Deployment of $service_name failed. Check $service_log_file." | tee -a "$LOG_FILE"
    exit 1
  fi

  # Fetch service URL
  echo "Fetching $service_name service URL..." | tee -a "$LOG_FILE"
  local service_url
  service_url=$(gcloud run services describe "$service_name" \
    --platform managed \
    --region "$REGION" \
    --project "$PROJECT_ID" \
    --format 'value(status.url)' 2>> "$service_log_file")
  if [ -z "$service_url" ]; then
    echo "Error: Failed to fetch service URL for $service_name." | tee -a "$LOG_FILE"
    exit 1
  fi
  renderer_url="${service_url}/render"

  # Update all project config files with the service URL
  for config_file in "${PROJECT_CONFIGS[@]}"; do
    if [ -f "$config_file" ]; then
      echo "Updating $config_file with web-renderer URL: $renderer_url..." | tee -a "$LOG_FILE"
      if ! jq ".web_renderer_url = \"$renderer_url\"" "$config_file" > "${config_file}.tmp" || ! mv "${config_file}.tmp" "$config_file"; then
        echo "Error: Failed to update $config_file with web-renderer URL." | tee -a "$LOG_FILE"
        exit 1
      fi
    else
      echo "Warning: Config file $config_file not found, skipping update." | tee -a "$LOG_FILE"
    fi
  done

  echo "$service_name deployed successfully." | tee -a "$LOG_FILE"
  echo "Service URL: $service_url" | tee -a "$LOG_FILE"
}

# Verify gcloud authentication
echo "Verifying gcloud authentication..." | tee -a "$LOG_FILE"
if ! gcloud config set project "$PROJECT_ID" >> "$LOG_FILE" 2>&1; then
  echo "Error: Failed to set project $PROJECT_ID." | tee -a "$LOG_FILE"
  exit 1
fi

# Check quota project
check_quota_project

# Create Artifact Registry repository if it doesn't exist
echo "Checking for Artifact Registry repository..." | tee -a "$LOG_FILE"
if ! gcloud artifacts repositories describe web-renderer-repo \
  --location "$REGION" \
  --project "$PROJECT_ID" >> "$LOG_FILE" 2>&1; then
  echo "Creating web-renderer-repo repository..." | tee -a "$LOG_FILE"
  gcloud artifacts repositories create web-renderer-repo \
    --repository-format=docker \
    --location "$REGION" \
    --project "$PROJECT_ID" >> "$LOG_FILE" 2>&1
fi

# Deploy web-renderer Cloud Run service
deploy_web_renderer

# Deploy Cloud Functions
echo "Checking for Pub/Sub topics..." | tee -a "$LOG_FILE"
topics_to_ensure=(
  "start-ingest-category-urls-topic"
  "start-asic-ingest-category-urls-topic"
  "start-analyze-website-schema-topic"
  "discover-main-urls-topic"
  "extract-initial-metadata-topic"
  "fetch-content-topic"
  "generate-xml-topic"
  "generate-reports-topic"
  "retry-pipeline"
)

for topic in "${topics_to_ensure[@]}"; do
  if ! gcloud pubsub topics describe "$topic" --project "$PROJECT_ID" >> "$LOG_FILE" 2>&1; then
    echo "Creating $topic topic..." | tee -a "$LOG_FILE"
    gcloud pubsub topics create "$topic" --project "$PROJECT_ID" >> "$LOG_FILE" 2>&1
  fi
done

# Deploy Cloud Functions with corrected entry points
deploy_single_function "ingest-category-urls" "ingest_category_urls_pubsub" "topic" "start-ingest-category-urls-topic" "functions/ingest_category_urls" "256MB" "300"
deploy_single_function "ingest-category-urls-asic" "ingest_category_urls_pubsub" "topic" "start-asic-ingest-category-urls-topic" "functions/ingest_category_urls" "256MB" "300"
deploy_single_function "analyze-website-schema" "analyze_website_schema" "topic" "start-analyze-website-schema-topic" "functions/analyze_website_schema" "1GB" "540"
deploy_single_function "discover-main-urls" "discover_main_urls" "topic" "discover-main-urls-topic" "functions/discover_main_urls" "1GB" "540"
deploy_single_function "extract-initial-metadata" "extract_initial_metadata" "topic" "extract-initial-metadata-topic" "functions/extract_initial_metadata" "1GB" "540"
deploy_single_function "fetch-content" "fetch_content" "topic" "fetch-content-topic" "functions/fetch_content" "512MB" "540"
deploy_single_function "generate-xml" "generate_xml" "topic" "generate-xml-topic" "functions/generate_xml" "512MB" "540"
deploy_single_function "generate-reports" "generate_reports" "topic" "generate-reports-topic" "functions/generate_reports" "512MB" "540"
deploy_single_function "retry-pipeline" "retry_pipeline" "topic" "retry-pipeline" "functions/retry_pipeline" "1GB" "300"

echo "All deployments completed!" | tee -a "$LOG_FILE"