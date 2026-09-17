from pathlib import Path
import py_compile


ROOT = Path(__file__).resolve().parents[1]


def test_project_python_files_compile() -> None:
    files = list((ROOT / "jobs").glob("*.py")) + list((ROOT / "airflow" / "dags").glob("*.py"))
    assert files
    for path in files:
        py_compile.compile(str(path), doraise=True)
