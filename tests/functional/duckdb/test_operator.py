from pathlib import Path

from detool.engine.__abc import Result
from detool.operator import Operator


def test_duckdb_csv_to_console(root_path: Path):
    op = Operator(path=root_path / "assets/example.duckdb.csv.detool")
    rs: Result = op.execute(allow_raise=True)
    print(rs.columns)


def test_duckdb_json_to_console(root_path: Path):
    op = Operator(path=root_path / "assets/example.duckdb.json.detool")
    rs: Result = op.execute(allow_raise=True)
    print(rs.columns)
