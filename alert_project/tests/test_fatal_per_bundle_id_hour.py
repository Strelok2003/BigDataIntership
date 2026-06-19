from src.rules.fatal_per_bundle_id_hour import FatalPerBundleIdHourRule
import pandas as pd


def test_alert_triggered(tmp_path):
    rule = FatalPerBundleIdHourRule(tmp_path)

    now = pd.Timestamp.now().floor("h")

    df = pd.DataFrame(
        {
            "severity": ["Error"] * 11,
            "date": [now] * 11,
            "bundle_id": ["my_real_bundle_id"] * 11,
        }
    )

    tmp_file_name = str(tmp_path / "file.csv")

    chunk = 0

    message = rule.process(df, tmp_file_name, chunk)

    assert message is not None


def test_alert_not_triggered(tmp_path):
    rule = FatalPerBundleIdHourRule(tmp_path)

    now = pd.Timestamp.now().floor("h")

    df = pd.DataFrame(
        {
            "severity": ["Error"] * 5,
            "date": [now] * 5,
            "bundle_id": ["my_real_bundle_id"] * 5,
        }
    )

    tmp_file_name = str(tmp_path / "file.csv")

    chunk = 0

    message = rule.process(df, tmp_file_name, chunk)

    assert message is None


def test_state_accumulates(tmp_path):
    rule = FatalPerBundleIdHourRule(tmp_path)

    now = pd.Timestamp.now().floor("h")

    df = pd.DataFrame(
        {
            "severity": ["Error"] * 6,
            "date": [now] * 6,
            "bundle_id": ["my_real_bundle_id"] * 6,
        }
    )

    tmp_file_name = str(tmp_path / "file.csv")

    chunk = 0

    assert rule.process(df, tmp_file_name, chunk) is None

    chunk = 1

    # Second run in same minute
    message = rule.process(df, tmp_file_name, chunk)

    assert message is not None


def test_non_error_records_ignored(tmp_path):
    rule = FatalPerBundleIdHourRule(tmp_path)

    now = pd.Timestamp.now().floor("min")

    df = pd.DataFrame(
        {
            "severity": ["Info"] * 20,
            "date": [now] * 20,
            "bundle_id": ["my_real_bundle_id"] * 20,
        }
    )

    tmp_file_name = str(tmp_path / "file.csv")

    chunk = 0

    message = rule.process(df, tmp_file_name, chunk)

    assert message is None
