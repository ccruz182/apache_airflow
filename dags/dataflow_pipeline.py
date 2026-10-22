from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowStartFlexTemplateOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

GCP_CONN = "google_cloud_default"

@dag(
    dag_id="dataflow_orders_pipeline",
    start_date=datetime(2026, 10, 21),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["dataflow", "gcp"],
)
def dataflow_pipeline():

    wait_for_file = GCSObjectExistenceSensor(
        task_id="wait_for_input_file",
        bucket="{{ var.value.gcs_data_bucket }}",
        object="data/orders.csv",
        google_cloud_conn_id=GCP_CONN,
        timeout=60 * 60 * 2,
        deferrable=True,
    )

    run_dataflow = DataflowStartFlexTemplateOperator(
        task_id="run_dataflow_job",
        project_id="{{ var.value.gcp_project_id }}",
        location="us-central1",
        body={
            "launchParameter": {
                "jobName": "orders-pipeline-{{ ds_nodash }}",
                "containerSpecGcsPath": "gs://{{ var.value.gcs_flex_templates_bucket }}/orders_pipeline.json",
                "parameters": {
                    "input": "gs://{{ var.value.gcs_data_bucket }}/data/orders.csv",
                    "output": "{{ var.value.gcp_project_id }}:analytics.completed_orders",
                },
                "environment": {
                    "tempLocation": "{{ var.value.gcs_flex_templates_temp_bucket }}",
                    "serviceAccountEmail": "{{ var.value.sa_dataflow }}",
                },
            }
        },
        gcp_conn_id=GCP_CONN,
        deferrable=True,
    )

    @task
    def notify_completion(ti=None) -> None:
        job_id = ti.xcom_pull(task_ids="run_dataflow_job", key="job_id")
        print(f"Dataflow job {job_id} completed successfully.")

    wait_for_file >> run_dataflow >> notify_completion()


dag_instance = dataflow_pipeline()
