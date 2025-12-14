from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="flights_avg_delay_daily",
    default_args=default_args,
    start_date=datetime(2025, 12, 13),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    run_spark_job = BashOperator(
        task_id="run_spark_flights_avg_delay",
        bash_command=(
            "docker exec spark-master "
            "/spark/bin/spark-submit --master spark://spark-master:7077 "
            "/spark_query.py"
        ),
    )

    run_spark_job

