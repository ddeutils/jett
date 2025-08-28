from pathlib import Path

from jett.engine.__abc import Result
from jett.tools import Tool


def test_arrow_csv_to_console(root_path: Path):
    op = Tool(path=root_path / "assets/example.arrow.csv.tool")
    rs: Result = op.execute(allow_raise=True)
    print(rs.columns)


def test_arrow_json_to_console(root_path: Path):
    op = Tool(path=root_path / "assets/example.arrow.json.tool")
    rs: Result = op.execute(allow_raise=True)
    print(rs.columns)
