from src.utils import list_csv_files, load_all_csvs, send_alert, move_file, build_rules

from src.rules.fatal_per_minute import FatalPerMinuteRule

from pathlib import Path


def test_list_csv_files(tmp_path):
    csv_file1 = tmp_path / "data1.csv"
    csv_file2 = tmp_path / "data2.csv"
    txt_file = tmp_path / "notes.txt"

    csv_file1.touch()
    csv_file2.touch()
    txt_file.touch()

    result = list_csv_files(str(tmp_path))

    assert len(result) == 2
    assert csv_file1 in result
    assert csv_file2 in result
    assert txt_file not in result


def test_load_all_csvs_success(tmp_path):
    f1 = tmp_path / "a.csv"
    f1.write_text("x,y\n1,2\n")

    f2 = tmp_path / "b.csv"
    f2.write_text("x,y\n3,4\n")

    failed_path = tmp_path / "failed"

    df = load_all_csvs([f1, f2], failed_path)

    assert len(df) == 2
    assert list(df["x"]) == [1, 3]


def test_load_all_csvs_missing_file(tmp_path):
    f1 = tmp_path / "a.csv"
    f1.write_text("x,y\n1,2\n")

    missing = tmp_path / "missing.csv"

    failed_path = tmp_path / "failed"

    df = load_all_csvs([f1, missing], failed_path)

    assert len(df) == 1


def test_load_all_csvs_empty(tmp_path):

    failed_path = tmp_path / "failed"

    df = load_all_csvs([], failed_path)

    assert df.empty


def test_send_alert():
    mock_http = type("MockHttp", (), {"request": lambda self, **kwargs: "ok"})()

    result = send_alert("https://example.com", "hello", mock_http)

    assert result == "ok"


def test_move_file_success(tmp_path):
    source = tmp_path / "file.csv"
    source.write_text("x,y\n1,2\n")

    dest_dir = tmp_path / "processed"

    result = move_file(source, dest_dir)

    # file moved
    assert not source.exists()
    assert (dest_dir / "file.csv").exists()

    # return value is correct
    assert result == dest_dir / "file.csv"


def test_build_rules_returns_expected_rules(tmp_path: Path):
    rules = build_rules(tmp_path)

    assert isinstance(rules, list)
    assert len(rules) == 1

    assert isinstance(rules[0], FatalPerMinuteRule)
