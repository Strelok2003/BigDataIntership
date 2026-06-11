from pathlib import Path
import pandas as pd
import logging
from json import dumps
from httplib2 import Http

from typing import Any


logger = logging.getLogger(__name__)


def list_csv_files(folder: str) -> list[Path]:
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


def load_all_csvs(files: list[Path]) -> pd.DataFrame:
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

    if not dataframes:
        return pd.DataFrame()

    return pd.concat(dataframes, ignore_index=True)


def send_alert(url: str, message: str) -> Any:
    """
    Send an alert message to a Google Chat webhook.

    This function creates a JSON payload containing the provided message
    and sends it as an HTTP POST request to the specified Google Chat
    webhook URL.

    Args:
        url (str): Google Chat webhook URL that will receive the alert.
        message (str): Alert message text to send.

    Returns:
        Any: The response returned by the underlying HTTP client request.
        Typically contains the HTTP response metadata and content.
    """
    app_message = {"text": message}
    message_headers = {"Content-Type": "application/json; charset=UTF-8"}
    http_obj = Http()

    response = http_obj.request(
        uri=url,
        method="POST",
        headers=message_headers,
        body=dumps(app_message),
    )

    return response
