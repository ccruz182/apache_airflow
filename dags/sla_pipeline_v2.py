import logging
import time
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

logger = logging.getLogger(__name__)


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    msg = (
        f"[SLA MISS] DAG: {dag.dag_id} | "
        f"Tasks: {task_list} | "
        f"Blocking: {blocking_task_list}"
    )
    print(msg)
    logger.warning(msg)

    for sla in slas:
        detail = (
            f"[SLA MISS DETAIL] task_id={sla.task_id} | "
            f"execution_date={sla.execution_date} | "
            f"timestamp={sla.timestamp}"
        )
        print(detail)
        logger.warning(detail)


@dag(
    dag_id="sla_pipeline_v2",
    start_date=pendulum.datetime(2026, 10, 22, tz="UTC"),
    schedule="*/5 * * * *",
    catchup=False,
    sla_miss_callback=sla_miss_callback,
    tags=["example", "sla"],
)
def sla_pipeline():

    @task(sla=timedelta(seconds=30),)
    def long_running_task() -> None:
        print("Starting test task; sleeping for 90 seconds.")
        logger.info("Starting test task; sleeping for 90 seconds.")
        time.sleep(90)
        print("Test task finished.")
        logger.info("Test task finished.")

    long_running_task()

sla_pipeline()
