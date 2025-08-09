from pathlib import Path

import pytest

from detool.core import Operator
from detool.engine.__abc import Result

from ...utils import SPARK_DISABLE


@pytest.mark.skipif(SPARK_DISABLE, reason="Spark testing does not enable.")
def test_spark_csv_to_console(root_path: Path):
    op = Operator(path=root_path / "assets/example.spark.csv.detool")
    rs: Result = op.execute(allow_raise=True)
    print(rs.columns)


@pytest.mark.skipif(SPARK_DISABLE, reason="Spark testing does not enable.")
def test_spark_json_to_console(root_path: Path):
    op = Operator(path=root_path / "assets/example.spark.json.detool")
    rs: Result = op.execute(allow_raise=True)
    print(rs.columns)
