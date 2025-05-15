#!/bin/bash
PROJECT_ID="execo-harvey"
REGION="europe-west4"
SERVICE_ACCOUNT="firebase-adminsdk-fbsvc@$PROJECT_ID.iam.gserviceaccount.com"

gcloud functions deploy extract-metadata \
  --runtime=python39 \
  --trigger-topic=process-data \
  --source=src/functions/extract_metadata \
  --project=$PROJECT_ID \
  --region=$REGION \
  --service-account=$SERVICE_ACCOUNT

gcloud functions deploy store-html \
  --runtime=python39 \
  --trigger-topic=store-html \
  --source=src/functions/store_html \
  --project=$PROJECT_ID \
  --region=$REGION \
  --service-account=$SERVICE_ACCOUNT