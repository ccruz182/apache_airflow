from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task, task_group
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud import bigquery
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from airflow.models import Param

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'dynamic_task_mapping_pipeline',
    default_args=default_args,
    description='Dynamic Task Mapping Pipeline',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    params={
        'start_date': Param(default='2014-01-01', type='string'),
        'end_date': Param(default='2014-12-31', type='string')
    },
    max_active_tasks=3
) as dag:

    # Parámetros GCP
    bucket_name = "{{ var.value.gcs_data_bucket }}"
    csv_file = 'data/store.csv'

    # Esquemas
    raw_schema = 'raw'
    stg_schema = 'stg'

    # Tabla
    table_name = 'store'

    # SQL query para crear la tabla externa con esquema inferido
    create_raw_table_query = f"""
    CREATE OR REPLACE EXTERNAL TABLE `{raw_schema}.{table_name}`
    (
        ID INTEGER,
        Order_ID STRING,
        Order_Date STRING,
        Ship_Date STRING,
        Ship_Mode STRING,
        Customer_ID STRING,
        Customer_Name STRING,
        Segment STRING,
        Country STRING,
        City STRING,
        State STRING,
        Postal_Code INTEGER,
        Region STRING,
        Product_ID STRING,
        Category STRING,
        Sub_Category STRING,
        Product_Name STRING,
        Sales NUMERIC,
        Quantity INTEGER,
        Discount NUMERIC,
        Profit NUMERIC
    )
    OPTIONS (
      format = 'CSV',
      uris = ['gs://{bucket_name}/{csv_file}'],
      skip_leading_rows = 1
    )
    """

    # Task para crear la tabla externa
    create_raw_table = BigQueryInsertJobOperator(
        task_id='create_raw_table',
        configuration={
            "query": {
                "query": create_raw_table_query,
                "useLegacySql": False,
            }
        },
    )

    @task
    def generate_date_ranges(start_date_str: str, end_date_str: str):
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

        date_ranges = []
        current_date = start_date

        while current_date <= end_date:
            next_month_date = current_date + relativedelta(months=1)
            date_ranges.append({
                'start_date': current_date.strftime('%Y-%m-%d'),
                'end_date': (next_month_date - timedelta(days=1)).strftime('%Y-%m-%d')
            })
            current_date = next_month_date

        print("DATE_RANGES ->", date_ranges)
        return date_ranges

    @task
    def load_stg_table(date_range: dict):
        start_date = date_range['start_date']
        end_date = date_range['end_date']

        create_stg_table_query = f"""
        CREATE TABLE IF NOT EXISTS `{stg_schema}.{table_name}` (
            ID INTEGER,
            Order_ID STRING,
            Order_Date DATE,
            Ship_Date DATE,
            Ship_Mode STRING,
            Customer_ID STRING,
            Customer_Name STRING,
            Segment STRING,
            Country STRING,
            City STRING,
            State STRING,
            Postal_Code INTEGER,
            Region STRING,
            Product_ID STRING,
            Category STRING,
            Sub_Category STRING,
            Product_Name STRING,
            Sales NUMERIC(20,4),
            Quantity INTEGER,
            Discount NUMERIC(20,4),
            Profit NUMERIC(20,4)
        )
        PARTITION BY DATE_TRUNC(Order_Date, MONTH)
        """

        load_stg_table_query = f"""
        INSERT INTO `{stg_schema}.{table_name}`
        SELECT 
            ID,
            Order_ID,
            CAST(Order_Date AS DATE FORMAT 'DD/MM/YYYY') Order_Date,
            CAST(Ship_Date AS DATE FORMAT 'DD/MM/YYYY') Ship_Date,
            Ship_Mode,
            Customer_ID,
            Customer_Name,
            Segment,
            Country,
            City,
            State,
            Postal_Code,
            Region,
            Product_ID,
            Category,
            Sub_Category,
            Product_Name,
            Sales,
            Quantity,
            Discount,
            Profit
        FROM `{raw_schema}.{table_name}`
        WHERE CAST(Order_Date AS DATE FORMAT 'DD/MM/YYYY') >= '{start_date}' 
          AND CAST(Order_Date AS DATE FORMAT 'DD/MM/YYYY') <= '{end_date}'
        """

        bq_client = bigquery.Client()
        bq_client.query(create_stg_table_query).result()
        bq_client.query(load_stg_table_query).result()

    # Generar rangos de fechas
    date_ranges = generate_date_ranges('{{ params.start_date }}', '{{ params.end_date }}')

    # Crear y encadenar tareas dinámicamente
    load_tasks = load_stg_table.expand(date_range=date_ranges)

    create_raw_table >> date_ranges >> load_tasks
