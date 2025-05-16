# Web Scraping Pipeline
A serverless web scraping pipeline for multiple customers and projects, using GCP and Apify.

## Setup
1. Clone the repository.
2. Configure GCP credentials and Apify API key.
3. Run `scripts/setup_gcp.sh` to create GCP resources.
4. Deploy Cloud Functions with `scripts/deploy_functions.sh`.

## Structure
- `src/functions/`: Cloud Functions for pipeline steps.
- `src/configs/`: Customer and project configurations.
- `src/common/`: Shared utilities and configuration loader.
- `tests/`: Unit tests for functions.
- `scripts/`: Deployment and setup scripts.

## Processing Steps
- **Extract Metadata**: Saves metadata to Firestore (`<customer>/<project>`).
- **Store HTML**: Saves HTML to GCS (`pending/<project>/<date>`).
- **Fix Image URLs**: Fixes image URLs, saves to GCS (`fixed/<project>/<date>`).
- **Generate XML**: Creates XML files, saves to GCS (`delivered_xml/<project>/<date>`).
- **Generate Reports**: Produces CSV reports, saves to GCS (`reports/<project>/<date>`).

## Deploying Cloud Functions
- Run `./scripts/deploy_functions.sh` to deploy functions using staging directories.
- Logs are saved to `logs/deploy_<timestamp>.log` and `logs/<function_name>_<timestamp>.log`.
- Ensure `src/common/utils.py` includes `setup_logging` with `google-cloud-logging==3.10.0` in `requirements.txt`.
- Use `apify-client==1.10.0` for `extract-metadata`.
- Includes `sftp-upload` for client file delivery.
- Use `--no-gen2` for 1st gen deployment.
- Authenticate ADC: `gcloud auth application-default login` if tokens expire.
- Set quota project: `gcloud auth application-default set-quota-project execo-harvey`.

## Adding a New Customer
1. Create `src/configs/customers/<customer_id>.json` with Apify key, Firestore DB, and GCP project ID.
2. Create a GCS bucket: `gs://<customer_id>-<region>`.
3. Update Cloud Scheduler with a new job for the customer.

## Adding a New Project
1. Create `src/configs/projects/<project_id>.json` with Apify dataset ID, GCS bucket, and required fields.
2. Test the pipeline with a small dataset.

## Monitoring
- Check Firestore for metadata: `harvey-netherlands/<customer_id>/<region>`.
- Check GCS for files: `gs://<customer_id>-<region>`.
- View logs in Cloud Logging.