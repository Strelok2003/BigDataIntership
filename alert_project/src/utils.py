from pathlib import Path
import pandas as pd
import logging


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
