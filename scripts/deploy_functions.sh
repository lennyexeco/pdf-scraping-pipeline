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

  gcloud functions deploy fix-image-urls \
  --runtime=python39 \
  --trigger-topic=fix-image-urls \
  --source=src/functions/fix_image_urls \
  --project=$PROJECT_ID \
  --region=$REGION \
  --service-account=$SERVICE_ACCOUNT

  gcloud functions deploy generate-xml \
  --runtime=python39 \
  --trigger-topic=generate-xml \
  --source=src/functions/generate_xml \
  --project=$PROJECT_ID \
  --region=$REGION \
  --service-account=$SERVICE_ACCOUNT

gcloud functions deploy generate-reports \
  --runtime=python39 \
  --trigger-topic=generate-reports \
  --source=src/functions/generate_reports \
  --project=$PROJECT_ID \
  --region=$REGION \
  --service-account=$SERVICE_ACCOUNT