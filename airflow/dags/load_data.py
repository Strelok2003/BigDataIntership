"""
Airflow DAG with responsibily of loading cleaned csv file to target collection of mongodb.
"""

from airflow.sdk import DAG, Asset
from airflow.providers.standard.operators.python import  PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from pendulum import DateTime, Timezone
import os
import pandas as pd


DATA_FOLDER = f"{os.getenv("AIRFLOW_HOME")}/Data"
FILE_NAME = "tiktok_google_play_reviews.csv"


def load_csv_to_mongo(file_path:str, collection_name:str) -> None:
    """
    Load data from a CSV file into a MongoDB collection.

    This function connects to a MongoDB instance using a predefined
    connection ID, clears the target collection, reads a CSV file
    into a pandas DataFrame, converts the data into a list of
    dictionaries, and inserts the records into the collection.

    Args:
        file_path (str): Path to the CSV file to be loaded.
        collection_name (str): target collection name.

    Returns:
        None
    """
    hook = MongoHook("mongo_conn")
    collection = hook.get_collection(collection_name)

    collection.delete_many({})

    df = pd.read_csv(file_path)
    payload = df.to_dict(orient="records")

    collection.insert_many(payload)


with DAG(
    dag_id="load_data",
    start_date= DateTime(2025, 1, 1, tzinfo=Timezone("Asia/Tbilisi")),
    schedule=[Asset("cleaned_csv")],
    catchup=False
):
    
    load_cleaned_data = PythonOperator(
        task_id="load_cleaned_data",
        python_callable=load_csv_to_mongo,
        op_args=[f"{DATA_FOLDER}/cleaned/{FILE_NAME}", "tiktok_google_play_reviews"]
    )

    load_cleaned_data
    