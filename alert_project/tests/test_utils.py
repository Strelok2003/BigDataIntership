from src.utils import list_csv_files, load_all_csvs


def test_list_csv_files(tmp_path):
    # 1. Arrange: Set up some mock files in the temporary directory
    csv_file1 = tmp_path / "data1.csv"
    csv_file2 = tmp_path / "data2.csv"
    txt_file = tmp_path / "notes.txt"

    # Actually create the blank files
    csv_file1.touch()
    csv_file2.touch()
    txt_file.touch()

    # 2. Act: Call your function using the temporary directory path
    # We pass str(tmp_path) because your function expects a string
    result = list_csv_files(str(tmp_path))

    # 3. Assert: Verify that only the CSV files were found
    assert len(result) == 2
    assert csv_file1 in result
    assert csv_file2 in result
    assert txt_file not in result


def test_load_all_csvs_success(tmp_path):
    f1 = tmp_path / "a.csv"
    f1.write_text("x,y\n1,2\n")

    f2 = tmp_path / "b.csv"
    f2.write_text("x,y\n3,4\n")

    df = load_all_csvs([f1, f2])

    assert len(df) == 2
    assert list(df["x"]) == [1, 3]


def test_load_all_csvs_missing_file(tmp_path):
    f1 = tmp_path / "a.csv"
    f1.write_text("x,y\n1,2\n")

    missing = tmp_path / "missing.csv"

    df = load_all_csvs([f1, missing])

    assert len(df) == 1


def test_load_all_csvs_empty():
    df = load_all_csvs([])

    assert df.empty
