from pathlib import Path
import pandas as pd
import logging
from json import dumps
from httplib2 import Http
import shutil

from src.rules.fatal_per_minute import FatalPerMinuteRule
from src.rules.fatal_per_bundle_id_hour import FatalPerBundleIdHourRule

from src.rules.base import BaseAlertRule

from typing import Any


logger = logging.getLogger(__name__)


def list_csv_files(folder: Path) -> list[Path]:
    """
    Return a list of all CSV files in the given folder.

    This function searches only the top-level of the specified directory
    (it does not search recursively) and returns all files ending with
    the `.csv` extension.

    Args:
        folder (str): Path to the directory to search for CSV files.

    Returns:
        list[Path]: A list of Path objects representing CSV files found
        in the directory. If no CSV files are found, returns an empty list.
    """
    return list(Path(folder).glob("*.csv"))


def load_all_csvs(files: list[Path], failed_folder: Path) -> pd.DataFrame:
    """
    Read multiple CSV files and concatenate them into a single DataFrame.

    Args:
        files (list[Path]): List of CSV file paths.

    Returns:
        pd.DataFrame: Combined DataFrame with all rows from all CSVs.
    """
    dataframes = []

    for file in files:
        if not file.exists():
            logger.warning(f"Skipping missing file: {file}")
            continue

        try:
            df = pd.read_csv(file)
            dataframes.append(df)
        except Exception as e:
            logger.error(f"Failed to read {file}: {e}")
            move_file(file, failed_folder)

    if not dataframes:
        return pd.DataFrame()

    return pd.concat(dataframes, ignore_index=True)


def send_alert(url: str, message: str, http_obj: Http = Http()) -> Any:
    """
    Send an alert message to a Google Chat webhook.

    This function creates a JSON payload containing the provided message
    and sends it as an HTTP POST request to the specified Google Chat
    webhook URL.

    Args:
        url (str): Google Chat webhook URL that will receive the alert.
        message (str): Alert message text to send.
        http_obj (Http, optional): HTTP client instance used to perform
            the request. Defaults to a new ``Http()`` instance. Can be
            overridden for testing or custom HTTP client configuration.

    Returns:
        Any: The response returned by the underlying HTTP client request.
        Typically contains the HTTP response metadata and content.
    """
    app_message = {"text": message}
    message_headers = {"Content-Type": "application/json; charset=UTF-8"}

    response = http_obj.request(
        uri=url,
        method="POST",
        headers=message_headers,
        body=dumps(app_message),
    )

    return response


def move_file(source: Path, destination_dir: Path) -> Path | None:
    """
    Move a file from source to destination directory.

    Args:
        source (Path): Path to the source file.
        destination_dir (Path): Target directory.

    Returns:
        Path: Final path of moved file.
    """

    if not source.exists():
        logger.warning(
            f"file does not exists to move, either already moved or deleted: {source}"
        )
        return None

    destination_dir.mkdir(parents=True, exist_ok=True)

    destination = destination_dir / source.name
    shutil.move(str(source), str(destination))

    return destination


def build_rules(state_folder_path: Path) -> list[BaseAlertRule]:
    return [
        FatalPerMinuteRule(state_folder_path),
        FatalPerBundleIdHourRule(state_folder_path),
        # more rules here
    ]
