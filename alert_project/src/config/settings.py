from configparser import ConfigParser
from pathlib import Path
import os

config = ConfigParser()

CONFIG_PATH = Path(__file__).parent / "default.cfg"

config.read(CONFIG_PATH)

GOOGLE_CHAT_WEBHOOK_URL = os.getenv("GOOGLE_CHAT_WEBHOOK_URL")

DATA_FOLDER_PATH = Path(str(os.getenv("DATA_FOLDER_PATH")))

INCOMING_FOLDER = DATA_FOLDER_PATH / config["folders"]["incoming_folder"]

FAILED_FOLDER = DATA_FOLDER_PATH / config["folders"]["failed_folder"]

PROCESSED_FOLDER = DATA_FOLDER_PATH / config["folders"]["processed_folder"]

STATE_FOLDER_PATH = Path(str(os.getenv("STATE_FOLDER_PATH")))


COLUMN_NAMES = [
    "error_code",
    "error_message",
    "severity",
    "log_location",
    "mode",
    "model",
    "graphics",
    "session_id",
    "sdkv",
    "test_mode",
    "flow_id",
    "flow_type",
    "sdk_date",
    "publisher_id",
    "game_id",
    "bundle_id",
    "appv",
    "language",
    "os",
    "adv_id",
    "gdpr",
    "ccpa",
    "country_code",
    "date",
]
