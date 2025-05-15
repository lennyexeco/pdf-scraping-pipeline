# Runbook: Web Scraping Pipeline
## Starting a Scrape
- Trigger via Cloud Scheduler or manually:
  ```bash
  gcloud pubsub topics publish process-data --message='{"customer": "<customer_id>", "project": "<project_id>", "dataset_id": "<dataset_id>"}'