from pathlib import Path

from jute.core import Operator
from jute.engine.__abc import Result


def test_duckdb_csv_to_console(root_path: Path):
    op = Operator(path=root_path / "assets/example.duckdb.csv.jute")
    rs: Result = op.execute(allow_raise=True)
    print(rs.columns)


def test_duckdb_json_to_console(root_path: Path):
    op = Operator(path=root_path / "assets/example.duckdb.json.jute")
    rs: Result = op.execute(allow_raise=True)
    print(rs.columns)
