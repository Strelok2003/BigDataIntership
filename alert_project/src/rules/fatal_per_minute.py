from src.rules.base import BaseAlertRule
import pandas as pd
from typing import Optional
from pathlib import Path


class FatalPerMinuteRule(BaseAlertRule):
    """
    Alert rule that triggers when more than 10 error-level log entries
    occur within a single minute.

    This rule maintains state across runs using a JSON state file to track:
    - the current minute being evaluated
    - the cumulative count of error logs in that minute

    The rule aggregates both:
    - previously recorded count for the minute (stateful)
    - new errors from the current batch of logs

    Once the total exceeds 10 within the same minute window,
    an alert message is returned.
    """

    def __init__(self, state_folder_path: Path):
        """
        Initialize the rule with a path to store state.

        Args:
            state_folder_path (Path): Directory where the state JSON file
                will be stored. The file will be named 'fatal_per_minute.json'.
        """
        state_file = state_folder_path / "fatal_per_minute.json"
        super().__init__(state_file)

    def process(
        self, df: pd.DataFrame, file_name: str, chunk_number: int
    ) -> Optional[str]:
        """
        Process a batch of log records and evaluate whether an alert
        should be triggered based on error frequency per minute.

        Args:
            df (pd.DataFrame): DataFrame containing log records.
                Expected columns:
                - 'severity': log severity level (e.g., "Error")
                - 'date': datetime column (must be pandas datetime dtype)

            file_name (str): Name of the source file being processed. Used for
                tracking progress and ensuring idempotent processing across retries.

            chunk_number (int): Sequential chunk index within the file. Used together
                with `file_name` to prevent duplicate processing in chunked pipelines.

        Returns:
            Optional[str]: Alert message if threshold is exceeded,
            otherwise None.
        """
        state = self._load_state()

        state_minute = state.get("minute")
        state_count = state.get("count", 0)

        processed_files, processed = self.check_processed_file(
            state, file_name, chunk_number
        )

        if processed:
            return None

        grouped = (
            df[df["severity"] == "Error"]
            .groupby(
                df["date"].dt.floor("min"),
            )
            .size()
        )

        if grouped.empty:
            return None

        grouped_latest_minute = grouped.index.get_level_values(0).max()

        if state_minute != str(grouped_latest_minute):
            state_count = 0

        latest_count = grouped.loc[grouped_latest_minute]

        total = state_count + int(latest_count)

        state = {
            "minute": str(grouped_latest_minute),
            "count": total,
            "processed_files": processed_files,
        }

        self._save_state(state)

        if total > 10:
            return f"ALERT: {total} fatal errors in minute {grouped_latest_minute}"

        return None
