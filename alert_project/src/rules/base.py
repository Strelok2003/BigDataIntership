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
                json.dump(state, f, indent=4)

        except (OSError, TypeError) as err:
            logger.error(f"failed to save state to {self._state_file}: {err}")

    @abstractmethod
    def process(
        self, df: pd.DataFrame, file_name: str, chunk_number: int
    ) -> Optional[str]:
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

            file_name (str): Name of the source file being processed. Used for
                tracking progress and ensuring idempotent processing across retries.

            chunk_number (int): Sequential chunk index within the file. Used together
                with `file_name` to prevent duplicate processing in chunked pipelines.

        Returns:
            Optional[str]: Alert message if triggered, otherwise None.
        """
        raise NotImplementedError

    def check_processed_file(
        self,
        state: dict,
        file_name: str,
        chunk_number: int,
    ) -> tuple[dict[str, int], bool]:
        """
        Check whether a file chunk has already been processed and update
        the processed-files tracking state.

        The function tracks the highest processed chunk number for each file.
        If the stored chunk number for the given file is greater than or equal
        to the current chunk number, the chunk is considered already processed.

        Args:
            state (dict): Current application state containing processing metadata.
                Expected structure:
                    {
                        "processed_files": {
                            "<file_name>": <last_processed_chunk>
                        }
                    }

            file_name (str): Name of the file being processed.

            chunk_number (int): Current chunk number for the file.

        Returns:
            tuple[dict[str, int], bool]:
                - Updated processed_files dictionary.
                - Boolean indicating whether the chunk was already processed.
                True means skip processing.
                False means chunk has not been processed yet.
        """
        processed_files = state.get("processed_files", {})

        processed = False

        if processed_files.get(file_name, -1) >= chunk_number:
            processed = True

        processed_files[file_name] = chunk_number

        return processed_files, processed
