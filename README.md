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
