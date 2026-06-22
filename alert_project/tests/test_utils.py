from src.utils import list_csv_files, send_alert, move_file, build_rules


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
