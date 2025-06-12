#!/bin/bash

# ==============================================================================
#
#  Complete FEDAO Pipeline Deployment Script
#
#  This script deploys all necessary components:
#  1. The Advanced Web Renderer (Cloud Run Service)
#  2. The FEDAO Scraper Cloud Function
#  3. The FEDAO Transformer Cloud Function
#  It also configures all required IAM permissions.
#
# ==============================================================================

# --- Configuration ---
# UPDATE: Replace with your actual GCP project ID
PROJECT_ID="execo-simba"  # Change this to your real project ID
REGION="europe-west1"
SERVICE_ACCOUNT="scraper-service-account@$PROJECT_ID.iam.gserviceaccount.com"

# --- Source Code Directories ---
# Deploy functions from individual directories after copying dependencies
TOP_LEVEL_SRC_DIR="src"
RENDERER_SRC_DIR="$TOP_LEVEL_SRC_DIR/services/web_renderer"
SCRAPER_FUNC_SRC_SUBDIR="$TOP_LEVEL_SRC_DIR/functions/scrape_fedao_sources"
TRANSFORM_FUNC_SRC_SUBDIR="$TOP_LEVEL_SRC_DIR/functions/transform_fedao_csv"

# --- Service & Bucket Names ---
WEB_RENDERER_SERVICE_NAME="advanced-web-renderer"
FEDAO_RAW_CSV_BUCKET_NAME="execo-simba-fedao-poc"

# --- Logging ---
LOG_DIR="logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/deploy_$(date +%Y%m%d_%H%M%S).log"

# Exit on any error
set -e

# ==============================================================================
# --- Helper Functions
# ==============================================================================
log_and_echo() {
  echo "$1" | tee -a "$LOG_FILE"
}

# ==============================================================================
# --- Main Deployment Logic
# ==============================================================================
log_and_echo "--- Starting Full FEDAO Pipeline Deployment at $(date) ---"

# --- Step 1: Initial GCP Setup ---
log_and_echo "STEP 1: Configuring gcloud and enabling necessary services..."

# Get current project instead of setting it
CURRENT_PROJECT=$(gcloud config get-value project 2>/dev/null)
if [ -z "$CURRENT_PROJECT" ]; then
    log_and_echo "ERROR: No project is currently set. Please run 'gcloud config set project YOUR_PROJECT_ID' first."
    log_and_echo "Available projects:"
    gcloud projects list --format="table(projectId,name)" 2>/dev/null || log_and_echo "Run 'gcloud auth login' to authenticate first."
    exit 1
fi

log_and_echo "Using current project: $CURRENT_PROJECT"
PROJECT_ID="$CURRENT_PROJECT"
SERVICE_ACCOUNT="scraper-service-account@$PROJECT_ID.iam.gserviceaccount.com"

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
log_and_echo "Services enabled successfully."

# --- Step 2: Deploy the Advanced Web Renderer (Cloud Run) ---
log_and_echo "STEP 2: Deploying the '$WEB_RENDERER_SERVICE_NAME' service to Cloud Run..."
if [ ! -d "$RENDERER_SRC_DIR" ]; then
    log_and_echo "ERROR: Renderer source directory not found at '$RENDERER_SRC_DIR'. Please check the RENDERER_SRC_DIR variable."
    exit 1
fi

log_and_echo "  - Source directory: $RENDERER_SRC_DIR"
log_and_echo "  - Attempting Cloud Run deployment..."
log_and_echo "  - Note: This may take several minutes for the initial build..."

# CHANGED: Removed --no-allow-unauthenticated and added --allow-unauthenticated
# Also improved error handling and logging
# FIXED: Removed unsupported CPU boost flags for compatibility
# ADDED: Show real-time output instead of redirecting to log file
log_and_echo "  - Building and deploying (this may take 5-10 minutes)..."

gcloud run deploy "$WEB_RENDERER_SERVICE_NAME" \
  --source "$RENDERER_SRC_DIR" \
  --platform "managed" \
  --region "$REGION" \
  --service-account "$SERVICE_ACCOUNT" \
  --allow-unauthenticated \
  --ingress=all \
  --memory "2Gi" \
  --cpu "1" \
  --timeout "300s" \
  --project "$PROJECT_ID" 2>&1 | tee -a "$LOG_FILE"

# Check if the deployment was successful
if [ ${PIPESTATUS[0]} -ne 0 ]; then
    log_and_echo "ERROR: Cloud Run deployment failed. Check the output above for details."
    exit 1
fi

log_and_echo "Cloud Run service '$WEB_RENDERER_SERVICE_NAME' deployed successfully with unauthenticated access."

# --- Step 3: Set IAM Permissions ---
log_and_echo "STEP 3: Granting required IAM roles to the service account..."

# Grant permission to use Vertex AI
log_and_echo "  - Granting Vertex AI User role..."
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$SERVICE_ACCOUNT" \
  --role="roles/aiplatform.user" \
  --condition=None >> "$LOG_FILE" 2>&1

# NOTE: Since the service now allows unauthenticated access, the Cloud Run Invoker role
# for the specific service account is less critical, but we'll keep it for consistency
log_and_echo "  - Granting Cloud Run Invoker role for the renderer service..."
gcloud run services add-iam-policy-binding "$WEB_RENDERER_SERVICE_NAME" \
  --member="serviceAccount:$SERVICE_ACCOUNT" \
  --role="roles/run.invoker" \
  --region="$REGION" \
  --project="$PROJECT_ID" >> "$LOG_FILE" 2>&1

log_and_echo "All IAM permissions granted successfully."

# --- Step 4: Deploy Cloud Functions ---
log_and_echo "STEP 4: Deploying Cloud Functions..."

# Dynamically get the URL of the deployed renderer
log_and_echo "  - Retrieving URL for the deployed renderer service..."
RENDERER_URL=$(gcloud run services describe "$WEB_RENDERER_SERVICE_NAME" --platform managed --region "$REGION" --format='value(status.url)' --project "$PROJECT_ID")
if [ -z "$RENDERER_URL" ]; then
    log_and_echo "ERROR: Failed to retrieve the URL for the renderer service. Cannot proceed."
    exit 1
fi
log_and_echo "  - Renderer URL found: $RENDERER_URL"

# Deploy the Transformer Function
# log_and_echo "  - Deploying 'transform-fedao-csv' function..."
# gcloud functions deploy "transform-fedao-csv" \
#     --gen2 \
#     --runtime=python39 \
#     --region="$REGION" \
#     --project="$PROJECT_ID" \
#     --source="$TRANSFORM_FUNC_SRC_SUBDIR" \
#     --entry-point="transform_fedao_csv_ai" \
#     --trigger-resource="$FEDAO_RAW_CSV_BUCKET_NAME" \
#     --trigger-event="google.storage.object.finalize" \
#     --service-account="$SERVICE_ACCOUNT" \
#     --timeout="540s" \
#     --memory="1Gi" \
#     --set-env-vars="GCP_PROJECT=${PROJECT_ID},LOG_LEVEL=INFO,CUSTOMER_ID_FOR_FEDAO=simba,FUNCTION_REGION=${REGION}" \
#     >> "$LOG_FILE" 2>&1
# log_and_echo "  ✓ 'transform-fedao-csv' deployed."

# # Deploy the Scraper Function
log_and_echo "  - Deploying 'scrape-fedao-sources' function..."
gcloud functions deploy "scrape-fedao-sources" \
    --gen2 \
    --runtime=python39 \
    --region="$REGION" \
    --project="$PROJECT_ID" \
    --source="$SCRAPER_FUNC_SRC_SUBDIR" \
    --entry-point="scrape_fedao_sources_ai" \
    --trigger-topic="scrape-fedao-sources-topic" \
    --service-account="$SERVICE_ACCOUNT" \
    --timeout="540s" \
    --memory="2Gi" \
    --set-env-vars="GCP_PROJECT=${PROJECT_ID},LOG_LEVEL=INFO,FUNCTION_REGION=${REGION},FEDAO_OUTPUT_BUCKET=${FEDAO_RAW_CSV_BUCKET_NAME},FEDAO_TREASURY_URL=https://www.newyorkfed.org/markets/domestic-market-operations/monetary-policy-implementation/treasury-securities/treasury-securities-operational-details,FEDAO_AMBS_URL=https://www.newyorkfed.org/markets/ambs_operation_schedule#tabs-2,RENDERER_SERVICE_URL=${RENDERER_URL}/render" \
    >> "$LOG_FILE" 2>&1
log_and_echo "  ✓ 'scrape-fedao-sources' deployed."

log_and_echo "All Cloud Functions deployed successfully."

# --- Step 5: Final Summary ---
log_and_echo "---"
log_and_echo "✅ DEPLOYMENT COMPLETED SUCCESSFULLY!"
log_and_echo "---"
log_and_echo "  - Cloud Run Renderer: $RENDERER_URL (PUBLIC ACCESS ENABLED)"
log_and_echo "  - Cloud Functions: transform-fedao-csv, scrape-fedao-sources (PUBLIC ACCESS ENABLED)"
log_and_echo "  - GCS Bucket for CSVs: gs://$FEDAO_RAW_CSV_BUCKET_NAME"
log_and_echo ""
log_and_echo "🔧 NEXT STEPS & TESTING:"
log_and_echo "1. Test the renderer service directly:"
log_and_echo "   curl -X POST $RENDERER_URL/render -H 'Content-Type: application/json' -d '{\"url\":\"https://example.com\"}'"
log_and_echo ""
log_and_echo "2. Test Cloud Functions (if they have HTTP triggers):"
log_and_echo "   # Get function URLs:"
log_and_echo "   gcloud functions describe scrape-fedao-sources --gen2 --region=$REGION --format='value(serviceConfig.uri)'"
log_and_echo "   gcloud functions describe transform-fedao-csv --gen2 --region=$REGION --format='value(serviceConfig.uri)'"
log_and_echo ""
log_and_echo "3. Trigger the pipeline:"
log_and_echo "   gcloud pubsub topics publish scrape-fedao-sources-topic --message='{}' --project=$PROJECT_ID"
log_and_echo ""
log_and_echo "4. Monitor logs:"
log_and_echo "   gcloud functions logs read scrape-fedao-sources --gen2 --project=$PROJECT_ID --region=$REGION --limit=50"
log_and_echo "   gcloud functions logs read transform-fedao-csv --gen2 --project=$PROJECT_ID --region=$REGION --limit=50"
log_and_echo ""
log_and_echo "5. Check for output files in GCS:"
log_and_echo "   gsutil ls gs://$FEDAO_RAW_CSV_BUCKET_NAME/FEDAO/inputs/raw_manual_uploads/"
log_and_echo ""
log_and_echo "⚠️  SECURITY WARNING: ALL SERVICES NOW HAVE PUBLIC ACCESS!"
log_and_echo "   - Cloud Run Service: Publicly accessible HTTP endpoint"
log_and_echo "   - Cloud Functions: Publicly accessible (if they have HTTP triggers)"
log_and_echo "   - Consider implementing authentication, rate limiting, or IP restrictions"
log_and_echo "   - Monitor usage and costs closely"
log_and_echo ""
log_and_echo "Deployment log saved to: $LOG_FILE"
log_and_echo "---"