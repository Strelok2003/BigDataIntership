"""
Airflow DAG for preprocessing TikTok Google Play review data.

This DAG monitors the arrival of a CSV file containing review data,
validates whether the file is empty, and performs a sequence of data
cleaning and transformation steps before producing a cleaned dataset.

Pipeline overview:
    1. Wait for the input CSV file to appear in the raw data directory.
    2. Check if the file is empty:
        - If empty, log a message and stop further processing.
        - If not empty, proceed with data cleaning.
    3. Data cleaning steps (TaskGroup: clean_data):
        a. Replace NULL values with a placeholder ("-").
        b. Sort the dataset by the "at" column (timestamp).
        c. Clean the "content" column by removing unwanted characters
           (e.g., emojis and non-text symbols), preserving only text
           and punctuation.
    4. Save the cleaned dataset to the "cleaned" directory.

Inputs:
    - Raw CSV file located at: ${AIRFLOW_HOME}/Data/raw/

Outputs:
    - Cleaned CSV file written to: ${AIRFLOW_HOME}/Data/cleaned/

Schedule:
    - Runs daily without backfilling (catchup=False).

Dependencies:
    - pandas
    - Apache Airflow (FileSensor, PythonOperator, BranchPythonOperator, BashOperator)

Notes:
    - Intermediate files are stored in a temporary directory.
    - File paths are passed between tasks using Airflow XCom.
"""

from airflow.sdk import DAG, Asset
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import TaskGroup
from pendulum import DateTime, Timezone
import os
import pandas as pd


DATA_FOLDER = f"{os.getenv("AIRFLOW_HOME")}/Data"
FILE_NAME = "tiktok_google_play_reviews.csv"



def branch_task(file_path:str) -> None:
    """
    Check whether specified file is emptry or not.

    Args:
        file_path (str): Path to the file we need to check.

    Returns:
        None
    """
    print(file_path)
    if os.path.getsize(file_path) == 0:
        return "empty_file_log"
    else:
        return "clean_data"


def replace_null_values(file_path:str, **context) -> None:
    """
    Replace NULL values with '-'

    This function reads csv file, replaces all NULL values with '-',
    writes data to temporary directory and pushes destination path
    to xcom for next transformations.

    Args:
        file_path (str): Path to the CSV file to be processed.
        **context: Airflow task context dictionary. Must contain a task instance
            (`ti`) used to pull the file path from XCom.

    Returns:
        None
    """

    df = pd.read_csv(file_path)

    df = df.fillna("-")

    destination_path = f"{DATA_FOLDER}/tmp/{FILE_NAME}"

    df.to_csv(destination_path, index=False)

    context["ti"].xcom_push(
        key="file_path",
        value=destination_path
    )



def sort_dataframe(**context) -> None:
    """
    Sort a CSV file by the "at" column.

    This function retrieves a file path from Airflow XCom, reads the CSV file
    into a pandas DataFrame, sorts the data by the "at" column, and overwrites
    the original file with the sorted data.

    Args:
        **context: Airflow task context dictionary. Must contain a task instance
            (`ti`) used to pull the file path from XCom.

    Returns:
        None
    """
    file_path = context["ti"].xcom_pull(
        task_ids="clean_data.replace_nulls",
        key="file_path"
    )

    df = pd.read_csv(file_path)

    df = df.sort_values(by="at")

    df.to_csv(file_path, index=False)


def clean_content_column(**context) -> None:
    """
    Clean the "content" column in a CSV file.

    This function retrieves a file path from Airflow XCom, reads the CSV file
    into a pandas DataFrame, and cleans the "content" column by removing all
    characters except alphanumeric characters, whitespace, and common
    punctuation marks. The cleaned data is then saved to a new file in the
    "cleaned" directory.

    Args:
        Args:
        **context: Airflow task context dictionary. Must contain a task instance
            (`ti`) used to pull the file path from XCom.

    Returns:
        None
    """
    file_path = context["ti"].xcom_pull(
        task_ids="clean_data.replace_nulls",
        key="file_path"
    )
    
    df = pd.read_csv(file_path)

    df["content"] = df["content"].str.replace(r"[^\w\s.,!?;:'\"()-]", "", regex=True)

    cleaned_file_path = f"{DATA_FOLDER}/cleaned/{FILE_NAME}"

    df.to_csv(cleaned_file_path, index=False)




with DAG(
    dag_id="prepare_data",
    start_date= DateTime(2025, 1, 1, tzinfo=Timezone("Asia/Tbilisi")),
    schedule="@daily",
    catchup=False
    ):
    
    
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        fs_conn_id="fs_default",
        filepath=f"{FILE_NAME}",
        mode="poke",
        poke_interval=10,
        timeout=60 * 10
    )


    file_empty_or_not = BranchPythonOperator(
        task_id="file_empty_or_not",
        python_callable=branch_task,
        op_args=[f"{DATA_FOLDER}/raw/{FILE_NAME}"]
    )


    empty_file_log = BashOperator(
        task_id="empty_file_log",
        bash_command="echo ""file is empty"""
    )


    with TaskGroup(
        group_id="clean_data"
    ) as clean_data:
        
        
        replace_nulls = PythonOperator(
            task_id="replace_nulls",
            python_callable=replace_null_values,
            op_kwargs={"file_path": f"{DATA_FOLDER}/raw/{FILE_NAME}"}
        )


        sort_data = PythonOperator(
            task_id="sort_data",
            python_callable=sort_dataframe
        )


        clean_content = PythonOperator(
            task_id="clean_content",
            python_callable=clean_content_column,
            outlets=[Asset("cleaned_csv")]
        )

        
        replace_nulls >> sort_data >> clean_content


    wait_for_file >> file_empty_or_not >> [empty_file_log, clean_data]