from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

GCP_CONN = "google_cloud_default"

@dag(
    dag_id="sensor_orders_pipeline",
    start_date=datetime(2026, 10, 21),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["sensors", "gcp"],
)
def sensor_pipeline():

    wait_for_file = GCSObjectExistenceSensor(
        task_id="wait_for_orders_file",
        bucket="{{ var.value.gcs_data_bucket }}",
        object="data/orders.csv",
        google_cloud_conn_id=GCP_CONN,
        timeout=60 * 60 * 2,
        deferrable=True,
    )

    load_to_bq = GCSToBigQueryOperator(
        task_id="load_orders_to_bq",
        bucket="{{ var.value.gcs_data_bucket }}",
        source_objects=["data/orders.csv"],
        destination_project_dataset_table="{{ var.value.gcp_project_id }}.raw.orders",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id=GCP_CONN,
        schema_fields=[
            {"name": "order_id",  "type": "INTEGER"},
            {"name": "customer",  "type": "STRING"},
            {"name": "amount",    "type": "FLOAT"},
            {"name": "status",    "type": "STRING"},
        ],
    )

    @task
    def notify_completion(table: str) -> None:
        print(f"Pipeline finished. Data available at {table}")

    wait_for_file >> load_to_bq >> notify_completion("{{ var.value.gcp_project_id }}.raw.orders")


dag_instance = sensor_pipeline()