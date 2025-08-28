from pathlib import Path

from jett.engine.__abc import Result
from jett.tools import Tool


def test_daft_csv_to_console(root_path: Path):
    op = Tool(path=root_path / "assets/example.daft.csv.tool")
    rs: Result = op.execute(allow_raise=True)
    print(rs.columns)
