import time
import logging
from src.utils import (
    list_csv_files,
    load_all_csvs,
    send_alert,
    move_file,
    build_rules,
)
from src.config.settings import (
    INCOMING_FOLDER,
    COLUMN_NAMES,
    PROCESSED_FOLDER,
    STATE_FOLDER_PATH,
    FAILED_FOLDER,
    GOOGLE_CHAT_WEBHOOK_URL,
)

import pandas as pd

logger = logging.getLogger(__name__)


def run_once():
    files = list_csv_files(INCOMING_FOLDER)

    if not files:
        return

    df = load_all_csvs(files, FAILED_FOLDER)
    df.columns = COLUMN_NAMES

    df["date"] = pd.to_datetime(df["date"], unit="s")

    rules = build_rules(STATE_FOLDER_PATH)

    for rule in rules:
        message = rule.process(df)
        if message:
            send_alert(
                GOOGLE_CHAT_WEBHOOK_URL,
                message,
            )

    for file in files:
        move_file(file, PROCESSED_FOLDER)


def main_loop():
    logging.info("Starting polling worker...")

    while True:
        try:
            run_once()
        except Exception as e:
            logging.error(f"Error in loop: {e}")

        time.sleep(3)


if __name__ == "__main__":
    main_loop()
