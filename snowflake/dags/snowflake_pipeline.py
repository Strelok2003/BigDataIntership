"""
Snowflake Data Ingestion Pipeline DAG

This Airflow DAG implements an end-to-end data ingestion workflow for Snowflake.
It dynamically discovers folders in a local directory, uploads files to a Snowflake stage,
and triggers a stored procedure to load the staged data into target tables.

Pipeline Flow:
---------------
1. Discover all subfolders inside a configured data directory (`DATA_FOLDER`).
2. For each folder:
   - Generate Snowflake `PUT` commands to upload files into a staged location (`RAW_STAGE`).
3. Execute all upload commands using Snowflake SQL operator.
4. For each folder:
   - Generate and execute a stored procedure call (`load_folder_data`)
     to load data from stage into Snowflake tables.

Dynamic Behavior:
------------------
- Uses Airflow TaskFlow API with dynamic task mapping (`expand` / `partial`).
- Each folder is processed independently in parallel.
- Folder names drive both staging path and target loading logic.

Configuration:
--------------
- DATA_FOLDER: Local directory containing source data folders.
- RAW_STAGE: Snowflake internal/external stage used for file ingestion.
- Connection: Uses `snowflake_conn` Airflow connection.

Tasks:
------
- get_folder_names: Discovers available data folders.
- generate_upload_script: Builds Snowflake PUT commands per folder.
- upload_files: Executes file upload commands in Snowflake.
- generate_call_command: Builds stored procedure calls per folder.
- copy_data: Executes data loading stored procedure in Snowflake.

"""

from airflow import DAG
from airflow.decorators import task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import DateTime, Timezone
import os

from config.config_reader import DATA_FOLDER, RAW_STAGE


@task
def get_folder_names(folder_path: str) -> list[str]:
    """
    Retrieve the names of all subfolders in a given directory.

    This function lists all entries in the specified directory and filters
    out only those that are directories (folders), returning their names.

    Args:
        folder_path (str): The path to the directory whose subfolders
            should be listed.

    Returns:
        list[str]: A list of folder names (not full paths) found in the
        specified directory.

    """
    folder_names = [
        name for name in os.listdir(folder_path)
        if os.path.isdir(os.path.join(folder_path, name))
    ]

    return folder_names


@task
def generate_upload_script(folder_name: str,
                            folder_path: str,
                            snowflake_stage: str) -> str:
    """
    Generate a Snowflake PUT command script for uploading files from a folder.

    This function constructs a Snowflake `PUT` command that uploads all files
    from a specified local folder into a given Snowflake stage directory.

    Args:
        folder_name (str): Name of the folder containing the files to upload.
        folder_path (str): Path to the parent directory containing the folder.
        snowflake_stage (str): Name of the Snowflake stage where files will
            be uploaded.

    Returns:
        str: A Snowflake `PUT` command string for uploading all files from the
        specified folder to the given stage.
    """

    files_to_upload = os.path.join(folder_path, folder_name, "*")

    script = f"PUT file://{files_to_upload}  @{snowflake_stage}/{folder_name}"

    return script


@task
def generate_call_command(folder_name:str, snowflake_stage:str) -> str:
    """
    Generate a Snowflake stored procedure CALL statement for loading folder data.

    This task constructs a SQL command that invokes the
    `load_folder_data` stored procedure with the provided
    table name, Snowflake stage, and folder name.

    Args:
        folder_name (str):
            Name of the source folder containing the data files.

        snowflake_stage (str):
            Name of the Snowflake stage where the files are located.

    Returns:
        str:
            A formatted SQL CALL statement.
    """
    call_command = f"CALL load_folder_data('{folder_name}', '{snowflake_stage}', '{folder_name}');"


    return call_command


with DAG(
    dag_id="snowflake_pipeline",
    start_date= DateTime(2025, 1, 1, tzinfo=Timezone("Asia/Tbilisi")),
    schedule="@daily",
    catchup=False
):
    
    folder_names = get_folder_names(DATA_FOLDER)

    upload_scripts = generate_upload_script.partial(
            folder_path=DATA_FOLDER,
            snowflake_stage=RAW_STAGE
    ).expand(folder_name=folder_names)


    upload_files = SQLExecuteQueryOperator(
        task_id="upload_files",
        conn_id="snowflake_conn",
        sql=upload_scripts
    )


    call_commands = generate_call_command.partial(
        snowflake_stage=RAW_STAGE
    ).expand(folder_name=folder_names)


    copy_data = SQLExecuteQueryOperator(
        task_id="copy_data",
        conn_id="snowflake_conn",
        sql=call_commands,
        autocommit=True
    )


    folder_names >> upload_scripts >> upload_files
    folder_names >> call_commands

    [upload_files, call_commands] >> copy_data