from configparser import ConfigParser
import os
from pathlib import Path

config = ConfigParser()

CONFIG_PATH = os.path.join(Path(__file__).parent, "config.cfg")

config.read(CONFIG_PATH)

FILE_NAME = config["files"]["file_name"]

COLLECTION_NAME = config["mongo_collections"]["collection_name"]

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")

DATA_FOLDER = os.path.join(AIRFLOW_HOME, config["paths"]["data_folder"])

TMP_FOLDER = os.path.join(DATA_FOLDER, config["paths"]["tmp_folder"])

CLEANED_FOLDER = os.path.join(DATA_FOLDER, config["paths"]["cleaned_folder"])

RAW_FOLDER = os.path.join(DATA_FOLDER, config["paths"]["raw_folder"])

RAW_FILE = os.path.join(RAW_FOLDER, FILE_NAME)