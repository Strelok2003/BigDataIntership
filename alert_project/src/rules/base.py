from abc import ABC, abstractmethod
import pandas as pd
from typing import Optional
from pathlib import Path
import logging
import json


logger = logging.getLogger(__name__)


class BaseAlertRule(ABC):
    """
    Abstract base class for alerting rules.

    An alert rule is responsible for maintaining any required state and
    evaluating log records against a specific alert condition.

    Implementations should define how state is loaded and persisted, as
    well as the logic used to analyze log data and determine whether an
    alert should be generated.
    """

    def __init__(self, state_file: Path):
        self._state_file = state_file

    def _load_state(self) -> dict:
        """
        Load the rule state from persistent storage.

        This method should retrieve any previously saved state required
        for evaluating the rule across processing runs, such as counters,
        timestamps, or rolling-window data structures.

        Returns:
            Any: The loaded state object. The exact type depends on the
                rule implementation.
        """

        if not self._state_file.exists():
            logger.warning(f"state file does not exists {self._state_file}")
            return {}

        try:
            with self._state_file.open("r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, dict) else {}
        except (json.JSONDecodeError, OSError) as err:
            logger.error(
                f"error occured while opening state file {self._state_file}: {err}"
            )
            return {}

    def _save_state(self, state: dict) -> None:
        """
        Persist the current rule state.

        This method should save any state necessary for future processing
        runs so that alert evaluation can continue correctly after the
        application restarts or processes a new batch of logs.

        Returns:
            None
        """
        try:
            with self._state_file.open("w", encoding="utf-8") as f:
                json.dump(state, f)

        except (OSError, TypeError) as err:
            logger.error(f"failed to save state to {self._state_file}: {err}")

    @abstractmethod
    def process(self, df: pd.DataFrame) -> Optional[str]:
        """
        Process log records and evaluate the alert rule.

        Implementations should analyze the provided log data, update
        internal state as needed, and determine whether alert conditions
        have been met.

        If the rule condition is satisfied, return an alert message.
        If no condition is met, return None.

        Args:
            df (pd.DataFrame): DataFrame containing log records to be
                evaluated by the rule.

        Returns:
            Optional[str]: Alert message if triggered, otherwise None.
        """
        raise NotImplementedError
