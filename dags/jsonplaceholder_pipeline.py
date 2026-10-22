# dags/sales_pipeline.py
from datetime import datetime
from airflow.decorators import dag, task
from operators.jsonplaceholdertogcs_operator import JsonPlaceholderToGCSOperator

@dag(
    dag_id="jsonplaceholder_pipeline",
    start_date=datetime(2026, 10, 21),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["custom", "gcp"],
)
def jsonplaceholder_pipeline():

    fetch_summary = JsonPlaceholderToGCSOperator(
        task_id="jsonplaceholder_posts",
        bucket="{{ var.value.gcs_data_bucket }}",
        gcs_path="output/jph_{{ ds }}.json", # Jinja
        jph_conn_id="jsonplaceholder",
        gcp_conn_id="google_cloud_default",
    )

    @task
    def log_output(gcs_path: str) -> None:
        print(f"File available at: {gcs_path}")

    fetch_summary >> log_output(fetch_summary.output)

dag_instance = jsonplaceholder_pipeline()