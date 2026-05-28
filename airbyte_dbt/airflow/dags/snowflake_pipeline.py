from airflow import DAG
from datetime import datetime
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.standard.operators.bash import BashOperator

import os

AIRBYTE_POSTGRES_TO_SNOWFLAKE_CONN_ID = os.getenv("AIRBYTE_POSTGRES_TO_SNOWFLAKE_CONN_ID")

DBT_PROJECT_DIR = '/opt/dbt/pagila_analytics'

with DAG(
    dag_id="snowflake_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    trigger_sync = AirbyteTriggerSyncOperator(
        task_id="trigger_airbyte_sync",
        airbyte_conn_id="airbyte_default",
        connection_id=AIRBYTE_POSTGRES_TO_SNOWFLAKE_CONN_ID,
    )

    dbt_debug = BashOperator(
        task_id='dbt_debug',
        cwd=DBT_PROJECT_DIR,
        bash_command='dbt debug'
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        cwd=DBT_PROJECT_DIR,
        bash_command='dbt run --select staging'
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        cwd=DBT_PROJECT_DIR,
        bash_command='dbt test'
    )

    trigger_sync >> dbt_debug >> dbt_run >> dbt_test