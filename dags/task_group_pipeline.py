from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule


GCP_CONN = "google_cloud_default"


@dag(
    dag_id="orders_task_group_pipeline",
    start_date=datetime(2026, 10, 21),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["orders", "gcp", "task-group"],
)
def orders_task_group_pipeline():

    wait_for_file = GCSObjectExistenceSensor(
        task_id="wait_for_orders_file",
        bucket="{{ var.value.gcs_data_bucket }}",
        object="data/orders.csv",
        google_cloud_conn_id=GCP_CONN,
        timeout=60 * 60,
        deferrable=True,
    )

    with TaskGroup(
        group_id="process_orders",
        tooltip="Validate and load the daily orders file",
    ) as process_orders:

        @task
        def validate_file() -> None:
            print("Validating orders.csv")
            # En un caso real:
            # - validar columnas
            # - comprobar que el archivo no está vacío
            # - validar tipos y valores permitidos

        load_raw = BigQueryInsertJobOperator(
            task_id="load_raw_to_bigquery",
            gcp_conn_id=GCP_CONN,
            configuration={
                "load": {
                    "sourceUris": [
                        "gs://{{ var.value.gcs_data_bucket }}/data/orders.csv"
                    ],
                    "destinationTable": {
                        "projectId": "{{ var.value.gcp_project_id }}",
                        "datasetId": "raw",
                        "tableId": "orders",
                    },
                    "sourceFormat": "CSV",
                    "skipLeadingRows": 1,
                    "writeDisposition": "WRITE_TRUNCATE",
                    "autodetect": False,
                    "schema": {
                        "fields": [
                            {"name": "order_id", "type": "INTEGER"},
                            {"name": "customer", "type": "STRING"},
                            {"name": "amount", "type": "FLOAT"},
                            {"name": "status", "type": "STRING"},
                        ]
                    },
                }
            },
        )

        validate_file() >> load_raw

    @task
    def transform_orders() -> None:
        print("Transforming raw.orders into analytics.completed_orders")

    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def notify_completion() -> None:
        print("Orders pipeline completed successfully")

    wait_for_file >> process_orders >> transform_orders() >> notify_completion()


orders_task_group_pipeline()
