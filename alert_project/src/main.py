import time
import logging
from src.utils import (
    list_csv_files,
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

    for file in files:
        try:
            rules = build_rules(STATE_FOLDER_PATH)

            for chunk in pd.read_csv(file, chunksize=10000):
                chunk.columns = COLUMN_NAMES

                chunk["date"] = pd.to_datetime(chunk["date"], unit="s")

                for rule in rules:
                    message = rule.process(chunk)
                    if message:
                        send_alert(
                            GOOGLE_CHAT_WEBHOOK_URL,
                            message,
                        )
        except Exception as e:
            logger.error(f"Failed to read {file}: {e}")
            move_file(file, FAILED_FOLDER)

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
