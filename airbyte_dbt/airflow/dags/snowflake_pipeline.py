from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
from airflow import DAG
from datetime import datetime

from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator

from config.config_reader import DBT_PROJECT_DIR, PROFILES_YML_FILEPATH

import os

AIRBYTE_POSTGRES_TO_SNOWFLAKE_CONN_ID = os.getenv("AIRBYTE_POSTGRES_TO_SNOWFLAKE_CONN_ID")


with DAG(
    dag_id="snowflake_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    
    trigger_sync = AirbyteTriggerSyncOperator(
        task_id="trigger_airbyte_sync",
        airbyte_conn_id="airbyte_default",
        connection_id=AIRBYTE_POSTGRES_TO_SNOWFLAKE_CONN_ID,
    )

    dbt_tasks = DbtTaskGroup(
        group_id="dbt_pagila_models",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_DIR
        ),
        profile_config=ProfileConfig(
            profile_name="pagila_analytics",
            target_name="dev",
            profiles_yml_filepath=PROFILES_YML_FILEPATH,
        ),
    )

    trigger_sync >> dbt_tasks