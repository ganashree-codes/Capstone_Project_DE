"""
airflow_dags/dbt_pipeline_dag.py

Orchestrates the Silver -> Gold transformation: runs dbt models, then dbt
tests, on a schedule. Assumes the dbt project is mounted/available inside
the Airflow container at DBT_PROJECT_DIR.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_BIN = '/opt/airflow/dbt_venv/bin/dbt' 
DBT_PROJECT_DIR = '/opt/airflow/dbt_project'
DBT_PROFILES_DIR = '/opt/airflow/dbt_project'
 
default_args = {
    'owner': 'data-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
 
DBT_FLAGS = f'--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} --no-partial-parse'

with DAG(
    dag_id='airflow_dbt_pipeline_dag_finshield',
    description='Runs dbt models (Silver -> Gold) and tests on a schedule',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['dbt', 'snowflake'],
) as dag:
 
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'{DBT_BIN} run {DBT_FLAGS} --full-refresh',
    )
 
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'{DBT_BIN} test {DBT_FLAGS}',
    )
 
   
    dbt_run >> dbt_test
