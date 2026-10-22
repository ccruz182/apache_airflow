from datetime import datetime
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook

import csv
import io

GCP_CONN = "google_cloud_default"

@dag(
    dag_id="taskflow_orders_pipeline",
    start_date=datetime(2026, 10, 21),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["taskflow", "gcp"],
)
def orders_pipeline():

    @task
    def extract_from_gcs() -> list[dict]:
        """Download CSV from GCS and return rows as a list of dicts."""
        hook = GCSHook(gcp_conn_id=GCP_CONN)
        content = hook.download("{{ var.value.gcs_data_bucket }}", "data/orders.csv")
        reader = csv.DictReader(io.StringIO(content.decode("utf-8")))
        return list(reader)

    @task
    def transform(rows: list[dict]) -> dict:
        """Compute a simple summary."""
        completed = [r for r in rows if r["status"] == "completed"]
        total_amount = sum(float(r["amount"]) for r in completed)
        return {
            "total_rows": len(rows),
            "completed_orders": len(completed),
            "completed_revenue": round(total_amount, 2),
        }

    @task
    def log_summary(summary: dict) -> None:
        print(f"Total rows     : {summary['total_rows']}")
        print(f"Completed      : {summary['completed_orders']}")
        print(f"Revenue        : ${summary['completed_revenue']}")

    rows    = extract_from_gcs()
    summary = transform(rows)
    log_summary(summary)


dag_instance = orders_pipeline()