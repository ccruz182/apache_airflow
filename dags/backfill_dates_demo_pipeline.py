import pendulum

from airflow.decorators import dag, task


@dag(
    dag_id="backfill_dates_demo",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["learning", "backfill"],
)
def backfill_dates_demo():

    @task
    def inspect_interval(**context):
        logical_date = context["logical_date"]
        data_interval_start = context["data_interval_start"]
        data_interval_end = context["data_interval_end"]

        print(f"run_id={context['run_id']}")
        print(f"logical_date={logical_date}")
        print(f"data_interval_start={data_interval_start}")
        print(f"data_interval_end={data_interval_end}")
        print(f"ds={context['ds']}")
        print(f"ds_nodash={context['ds_nodash']}")

    inspect_interval()


backfill_dates_demo()