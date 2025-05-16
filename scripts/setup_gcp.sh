#!/bin/bash

# Configuration variables
PROJECT_ID="web-scraping-pipeline"
REGION="europe-west4"
SERVICE_ACCOUNT="scraper-service-account"
FIRESTORE_DB="harvey-au"
BUCKET_PREFIX="harvey-netherlands"
CUSTOMER="harvey"
PROJECTS=("netherlands" "germany")
PUBSUB_TOPICS=("start-scrape" "process-data" "store-html" "fix-image-urls" "generate-xml" "generate-reports" "sftp-upload")
API_KEY_SECRET="apify-api-key"
API_KEY_VALUE="apify_api_o8Ib1KhGVJZmrClKTEFqnU2WR31ZtP0M28iN"

# Exit on error
set -e

# Set project
echo "Setting GCP project to $PROJECT_ID..."
gcloud config set project $PROJECT_ID

# Enable APIs
echo "Enabling required APIs..."
gcloud services enable \
  cloudfunctions.googleapis.com \
  pubsub.googleapis.com \
  cloudtasks.googleapis.com \
  cloudscheduler.googleapis.com \
  storage.googleapis.com \
  firestore.googleapis.com \
  logging.googleapis.com \
  secretmanager.googleapis.com

# Create Firestore database
echo "Creating Firestore database: $FIRESTORE_DB..."
gcloud firestore databases create --database=$FIRESTORE_DB --location=$REGION

# Create GCS buckets
for proj in "${PROJECTS[@]}"; do
  BUCKET_NAME="${BUCKET_PREFIX}-${proj}"
  echo "Creating GCS bucket: $BUCKET_NAME..."
  gsutil mb -l $REGION gs://$BUCKET_NAME
done

# Create Pub/Sub topics
for topic in "${PUBSUB_TOPICS[@]}"; do
  echo "Creating Pub/Sub topic: $topic..."
  gcloud pubsub topics create $topic
done

# Create Cloud Scheduler job
echo "Creating Cloud Scheduler job for $CUSTOMER-netherlands..."
gcloud scheduler jobs create pubsub daily-scrape-$CUSTOMER-netherlands \
  --schedule="0 2 * * *" \
  --topic=start-scrape \
  --message-body="{\"customer\": \"$CUSTOMER\", \"project\": \"netherlands\", \"dataset_id\": \"CebgWL0NgapQr9rRO\"}" \
  --location=$REGION

# Create service account
echo "Creating service account: $SERVICE_ACCOUNT..."
gcloud iam service-accounts create $SERVICE_ACCOUNT \
  --display-name="Scraper Service Account"

# Assign IAM roles
ROLES=(
  "roles/cloudfunctions.developer"
  "roles/storage.admin"
  "roles/datastore.user"
  "roles/pubsub.publisher"
  "roles/cloudscheduler.admin"
  "roles/secretmanager.secretAccessor"
)
for role in "${ROLES[@]}"; do
  echo "Assigning role $role to $SERVICE_ACCOUNT..."
  gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="$role"
done

# Store Apify API key in Secret Manager
echo "Storing Apify API key in Secret Manager..."
gcloud secrets create $API_KEY_SECRET --replication-policy="automatic" || true
echo -n $API_KEY_VALUE | gcloud secrets versions add $API_KEY_SECRET --data-file=-

# Optional: Download service account key (for local development)
# echo "Downloading service account key..."
# gcloud iam service-accounts keys create credentials.json \
#   --iam-account=$SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com

echo "GCP setup completed successfully!"