from airflow.sdk import DAG, Asset
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import TaskGroup
from pendulum import DateTime, Timezone
import os
import pandas as pd


DATA_FOLDER = "/home/modeb/airflow/Data"
FILE_NAME = "tiktok_google_play_reviews.csv"


def branch_task(file_path):
    print(file_path)
    if os.path.getsize(file_path) == 0:
        return "empty_file_log"
    else:
        return "clean_data"


def replace_null_values(file_path, **context):
    df = pd.read_csv(file_path)

    df = df.fillna("-")

    destination_path = f"{DATA_FOLDER}/tmp/{FILE_NAME}"

    df.to_csv(destination_path, index=False)

    context["ti"].xcom_push(
        key="file_path",
        value=destination_path
    )



def sort_dataframe(**context):
    file_path = context["ti"].xcom_pull(
        task_ids="clean_data.replace_nulls",
        key="file_path"
    )

    df = pd.read_csv(file_path)

    df = df.sort_values(by="at")

    df.to_csv(file_path, index=False)


def clean_content_column(**context):
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
        filepath=f"raw/{FILE_NAME}",
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