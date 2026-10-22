# Apache Airflow
## Pipelines
- Branching, Dataflow, Custom Hooks and Operators, SLAs, BigQuery.

## GCP Requirements
### Required Airflow Variables
- gcs_data_bucket
- gcp_project_id
- gcs_flex_templates_bucket
- gcs_flex_templates_temp_bucket
- sa_dataflow

### Required Airflow Connections
- google_cloud_default
- jsonplaceholder

### Required IAM Roles
- For the Dataflow SA, the Composer SA must have permission to impersonate.

### Required GCP APIs
- Cloud Storage
- BigQuery
- Dataflow
- Cloud Logging
- Cloud Monitoring
