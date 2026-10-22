import pendulum

from airflow.decorators import dag
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.sensors.gcs import (
    GCSObjectExistenceSensor,
)


PROJECT = "{{ var.value.gcp_project_id }}"
BUCKET = "{{ var.value.gcs_data_bucket }}"
GCP_CONN = "google_cloud_default"


@dag(
    dag_id="idempotent_orders_pipeline",
    start_date=pendulum.datetime(2026, 10, 20, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["learning", "idempotency", "bigquery"],
)
def idempotent_orders_pipeline():

    wait_for_file = GCSObjectExistenceSensor(
        task_id="wait_for_daily_file",
        bucket=BUCKET,
        object="data/orders.csv",
        google_cloud_conn_id=GCP_CONN,
        timeout=60 * 60,
        deferrable=True,
    )

    load_staging = BigQueryInsertJobOperator(
        task_id="load_staging",
        gcp_conn_id=GCP_CONN,
        configuration={
            "load": {
                "sourceUris": [
                    f"gs://{BUCKET}/data/orders.csv"
                ],
                "destinationTable": {
                    "projectId": PROJECT,
                    "datasetId": "staging",
                    "tableId": "orders_{{ ds_nodash }}",
                },
                "sourceFormat": "CSV",
                "skipLeadingRows": 1,
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
                "schema": {
                    "fields": [
                        {"name": "order_id", "type": "INTEGER"},
                        {"name": "customer", "type": "STRING"},
                        {"name": "amount", "type": "FLOAT"},
                        {"name": "status", "type": "STRING"},
                        {"name": "order_date", "type": "DATE"},
                    ]
                },
            }
        },
    )

    replace_partition = BigQueryInsertJobOperator(
        task_id="replace_target_partition",
        gcp_conn_id=GCP_CONN,
        configuration={
            "query": {
                "query": f"""
                    -- Elimina sólo la partición correspondiente al intervalo
                    DELETE FROM `{PROJECT}.raw.orders`
                    WHERE order_date = DATE('{{{{ ds }}}}');

                    -- Inserta nuevamente la partición completa
                    INSERT INTO `{PROJECT}.raw.orders`
                    SELECT
                        order_id,
                        customer,
                        amount,
                        status,
                        order_date
                    FROM `{PROJECT}.staging.orders_{{{{ ds_nodash }}}}`
                    WHERE order_date = DATE('{{{{ ds }}}}');
                """,
                "useLegacySql": False,
            }
        },
    )

    wait_for_file >> load_staging >> replace_partition


idempotent_orders_pipeline()
