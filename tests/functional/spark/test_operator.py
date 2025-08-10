from pathlib import Path

import pytest

from detool.engine.__abc import Result
from detool.operator import Operator

from ...utils import SPARK_DISABLE


@pytest.mark.skipif(SPARK_DISABLE, reason="Spark testing does not enable.")
def test_spark_csv_to_console(root_path: Path):
    op = Operator(path=root_path / "assets/example.spark.csv.tool")
    rs: Result = op.execute(allow_raise=True)
    print(rs.columns)


@pytest.mark.skipif(SPARK_DISABLE, reason="Spark testing does not enable.")
def test_spark_json_to_console(root_path: Path):
    op = Operator(path=root_path / "assets/example.spark.json.tool")
    rs: Result = op.execute(allow_raise=True)
    print(rs.columns)
