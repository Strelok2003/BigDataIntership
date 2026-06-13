from src.rules.base import BaseAlertRule
from pathlib import Path
import pandas as pd
from typing import Optional


class FatalPerBundleIdHourRule(BaseAlertRule):
    def __init__(self, state_folder_path: Path):
        """
        Initialize the rule with a path to store state.

        Args:
            state_folder_path (Path): Directory where the state JSON file
                will be stored. The file will be named 'fatal_per_bundle_id_hour.json'.
        """
        state_file = state_folder_path / "fatal_per_bundle_id_hour.json"
        super().__init__(state_file)

    def process(self, df: pd.DataFrame) -> Optional[str]:
        """
        Process a batch of log records and evaluate whether an alert
        should be triggered based on error frequency per hour and bundle_id.

        Args:
            df (pd.DataFrame): DataFrame containing log records.
                Expected columns:
                - 'severity': log severity level (e.g., "Error")
                - 'date': datetime column (must be pandas datetime dtype)
                - 'bundle_id': bundle identification

        Returns:
            Optional[str]: Alert message if threshold is exceeded,
            otherwise None.
        """
        now_hour = pd.Timestamp.now().floor("h")

        state = self._load_state()

        last_hour = state.get("hour")
        bundles = state.get("bundles", {})

        if last_hour != str(now_hour):
            bundles = {}

        current_counts = (
            df[(df["severity"] == "Error") & (df["date"].dt.floor("h") == now_hour)]
            .groupby("bundle_id")
            .size()
            .to_dict()
        )

        for bundle_id, count in current_counts.items():
            bundles[bundle_id] = bundles.get(bundle_id, 0) + count

        state = {"hour": str(now_hour), "bundles": bundles}

        self._save_state(state)

        messages = []

        for bundle_id, total in bundles.items():
            if total > 10:
                message = f"ALERT: {total} fatal errors in hour {now_hour} for bundle_id={bundle_id}"
                messages.append(message)

        if messages:
            return "\n\n".join(messages)

        return None
