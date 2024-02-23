"""
Simple DAG for testing purposes (https://airflow.apache.org/tutorial.html)
"""
from __future__ import annotations
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="dag_databricks",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=50)
    },
    description="A simple tutorial DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["test"],
) as dag:
    t1 = DatabricksRunNowOperator(
        task_id="run_now_task1",
        databricks_conn_id="databricks_default",
        job_id=737529890918698
    )

    t2 = DatabricksRunNowOperator(
        task_id="run_now_task2",
        databricks_conn_id="databricks_default",
        job_id=75006230170927
    )
    dag.doc_md = __doc__  # providing that you have a docstring at the beginning of the DAG; OR
    dag.doc_md = """
    This is a documentation placed anywhere
    """

    t1 >> t2


