from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

@dag(
    dag_id="branching_pipeline",
    start_date=datetime(2026, 10, 21),
    schedule="@daily",
    catchup=False,
)
def branching_pipeline():

    @task.branch
    def check_day(ds: str) -> str:
        day = datetime.strptime(ds, "%Y-%m-%d").strftime("%A")
        if day == "Monday":
            return "run_weekly_report"
        return "skip_weekly_report"

    @task
    def run_weekly_report() -> None:
        print("Running weekly report...")

    @task
    def skip_weekly_report() -> None:
        print("Not Monday, skipping.")

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def load_daily_data() -> None:
        print("Always runs regardless of branch.")

    branch        = check_day()
    weekly_report = run_weekly_report()
    skip          = skip_weekly_report()
    daily         = load_daily_data()

    branch >> [weekly_report, skip]
    [weekly_report, skip] >> daily

dag_instance = branching_pipeline()
