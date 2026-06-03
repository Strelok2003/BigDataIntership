from configparser import ConfigParser
import os
from pathlib import Path

config = ConfigParser()

CONFIG_PATH = os.path.join(Path(__file__).parent, "config.cfg")

config.read(CONFIG_PATH)

DBT_PROJECT_DIR = config["paths"]["dbt_project_dir"]

PROFILES_YML_FILEPATH = config["paths"]["profiles_yml_filepath"]