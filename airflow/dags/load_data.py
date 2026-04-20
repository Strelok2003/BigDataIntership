from airflow.sdk import DAG, Asset
from airflow.providers.standard.operators.python import  PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from pendulum import DateTime, Timezone
import os
import pandas as pd


DATA_FOLDER = "/home/modeb/airflow/Data"
FILE_NAME = "tiktok_google_play_reviews.csv"


def load_csv_to_mongo(file_path):
    hook = MongoHook("mongo_conn")

    df = pd.read_csv(file_path)

    payload = df.to_dict(orient="records")

    hook.insert_many("tiktok_google_play_reviews", payload)


with DAG(
    dag_id="load_data",
    start_date= DateTime(2025, 1, 1, tzinfo=Timezone("Asia/Tbilisi")),
    schedule=[Asset("cleaned_csv")],
    catchup=False
):
    
    load_cleaned_data = PythonOperator(
        task_id="load_cleaned_data",
        python_callable=load_csv_to_mongo,
        op_args=[f"{DATA_FOLDER}/cleaned/{FILE_NAME}"]
    )

    load_csv_to_mongo
    