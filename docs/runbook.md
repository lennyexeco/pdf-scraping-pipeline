# Runbook: Web Scraping Pipeline V2

This runbook outlines procedures for operating and managing the Web Scraping Pipeline.

## Starting a Scrape for a Project

There are two primary ways to initiate scraping for a configured project, typically after setting up its configuration files and uploading its initial category URLs to GCS.

**1. For a New Site (Recommended - Includes AI Schema Analysis):**

This process first analyzes the target website to generate dynamic parsing configurations (selectors, mappings, etc.) before starting the full ingestion.

* **Action:** Publish a message to the `start-analyze-website-schema-topic` Pub/Sub topic.
* **Payload Format (JSON):**
    ```json
    {
      "customer": "<customer_id_from_config>",
      "project": "<project_config_name_from_filename>",
      "csv_gcs_path": "gs://<your-gcs-bucket-name>/<project_config_name>/inputs/<your_category_urls.csv>"
    }
    ```
* **Example Command (`gcloud` CLI):**
    ```bash
    gcloud pubsub topics publish start-analyze-website-schema-topic --project YOUR_GCP_PROJECT_ID --message \
    '{
      "customer": "harvey",
      "project": "example_new_site",
      "csv_gcs_path": "gs://your-gcs-bucket-name/example_new_site/inputs/category_urls.csv"
    }'
    ```
* **Outcome:** This triggers the `analyze_website_schema` function. Upon successful completion, it will automatically publish another message to `start-ingest-category-urls-topic` to begin the main data ingestion and processing flow for the project.

**2. For Direct Ingestion (Skipping AI Schema Analysis):**

Use this method if an AI-generated schema already exists in Firestore for the project, or if you intend to rely solely on the static configurations defined in the project's JSON file.

* **Action:** Publish a message to the `start-ingest-category-urls-topic` Pub/Sub topic.
* **Payload Format (JSON):** (Same as for schema analysis)
    ```json
    {
      "customer": "<customer_id_from_config>",
      "project": "<project_config_name_from_filename>",
      "csv_gcs_path": "gs://<your-gcs-bucket-name>/<project_config_name>/inputs/<your_category_urls.csv>"
    }
    ```
* **Example Command (`gcloud` CLI):**
    ```bash
    gcloud pubsub topics publish start-ingest-category-urls-topic --project YOUR_GCP_PROJECT_ID --message \
    '{
      "customer": "harvey",
      "project": "example_new_site",
      "csv_gcs_path": "gs://your-gcs-bucket-name/example_new_site/inputs/category_urls.csv"
    }'
    ```
* **Outcome:** This triggers the `ingest_category_urls` function, which starts processing the CSV, and the pipeline proceeds using the best available configuration (AI-generated from Firestore or static).

## Handling Errors and Retries

* **Automatic Retries:** Most functions, upon failure, will publish a detailed error message to the `retry-pipeline` Pub/Sub topic.
* **`retry_pipeline` Function:** This function attempts to diagnose the error (with Vertex AI assistance) and re-trigger the failed stage.
* **Monitoring Retries:**
    * Check logs for the `retry_pipeline` function.
    * Examine the project's error collection in Firestore (e.g., `{project_firestore_collection}_errors/{url_hash}`) for retry history and LLM analysis.
* **Manual Retry:** If an item is stuck in an error state (e.g., "Unresolvable_Max_Retries"), manual investigation is needed. Depending on the issue, you might:
    * Fix the underlying data or configuration.
    * Manually adjust the error document in Firestore to allow for a new retry attempt (e.g., reset `retry_count`, change status) and then re-publish to the `retry-pipeline` topic with the relevant identifier and original stage.
    * Manually re-trigger the specific stage that failed by publishing to its input topic with the correct payload.

## Checking Pipeline Status

* **Cloud Logging:** View logs for all functions. Use filters for specific projects (`jsonPayload.project`), customers (`jsonPayload.customer`), or identifiers (`jsonPayload.identifier`).
* **Firestore:**
    * `projects/{project_config_name}`: For AI-generated `metadata_schema`.
    * `{project_firestore_collection}/`: For status of individual items (`processing_status`, `xml_status`, GCS paths).
* **GCS:** Verify the existence and content of input CSVs, processed HTML/PDFs, XMLs, and reports in the configured bucket and paths.

## Deployment
* Run `./deploy.sh` to deploy or update all Cloud Functions. This script also ensures necessary Pub/Sub topics are created.
* Ensure your local `gcloud` CLI is authenticated and configured for the correct GCP project.

## Emergency Stop
* To stop processing for a specific project or function, you can pause its Pub/Sub trigger in the Google Cloud Console (Pub/Sub > Subscriptions > Select Subscription > Pause).
* For a more complete stop, you might need to disable the initial trigger functions (e.g., the subscription for `start-analyze-website-schema-topic` or `start-ingest-category-urls-topic`).
* Be aware that messages already in Pub/Sub topics might still be processed until their acknowledgment deadlines.