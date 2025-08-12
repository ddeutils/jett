from pathlib import Path

from detool.engine.__abc import Result
from detool.tools import Tool


def test_polars_csv_to_console(root_path: Path):
    op = Tool(path=root_path / "assets/example.polars.csv.tool")
    rs: Result = op.execute(allow_raise=True)
    print(rs.columns)


def test_polars_json_to_console(root_path: Path):
    op = Tool(path=root_path / "assets/example.polars.json.tool")
    rs: Result = op.execute(allow_raise=True)
    print(rs.columns)
