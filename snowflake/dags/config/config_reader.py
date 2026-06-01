from configparser import ConfigParser
import os
from pathlib import Path

config = ConfigParser()

CONFIG_PATH = os.path.join(Path(__file__).parent, "config.cfg")

config.read(CONFIG_PATH)

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")

DATA_FOLDER = os.path.join(AIRFLOW_HOME, config["paths"]["data_folder"])

RAW_STAGE = config["snowflake"]["raw_stage"]