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

    def process(self, df: pd.DataFrame) -> Optional[str]:
        """
        Process a batch of log records and evaluate whether an alert
        should be triggered based on error frequency per minute.

        Args:
            df (pd.DataFrame): DataFrame containing log records.
                Expected columns:
                - 'severity': log severity level (e.g., "Error")
                - 'date': datetime column (must be pandas datetime dtype)

        Returns:
            Optional[str]: Alert message if threshold is exceeded,
            otherwise None.
        """

        now_minute = pd.Timestamp.now().floor("min")

        state = self._load_state()

        last_minute = state.get("minute")
        previous_count = state.get("count", 0)

        current_count = df[
            (df["severity"] == "Error") & (df["date"].dt.floor("min") == now_minute)
        ].shape[0]

        if last_minute != str(now_minute):
            previous_count = 0

        total = previous_count + current_count

        state = {"minute": str(now_minute), "count": total}

        self._save_state(state)

        if total > 10:
            return f"ALERT: {total} fatal errors in minute {now_minute}"

        return None
