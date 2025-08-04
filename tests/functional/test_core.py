import pytest

from jute.core import Operator, SparkSubmitOperator
from jute.errors import JuteCoreError


def test_operator():
    op = Operator(
        config={
            "type": "duckdb",
            "name": "load_csv",
            "source": {
                "type": "local",
                "path": "./file.csv",
                "file_format": "csv",
            },
            "sink": {"type": "console"},
        }
    )
    assert op.c.model.type == "duckdb"
    assert op.c.model.source.type == "local"
    assert op.c.model.source.file_format == "csv"
    assert op.c.model.sink.type == "console"

    op.c.data["type"] = "empty"
    op.c.data["sink"]["type"] = "empty"
    assert op.c.data["sink"]["type"] == "empty"
    assert op.c.data["type"] == "empty"

    with pytest.raises(JuteCoreError):
        op.c.fetch(op.c.data)

    op.refresh()

    assert op.c.model.type == "duckdb"
    assert op.c.model.source.type == "local"


def test_operator_raise():
    with pytest.raises(JuteCoreError):
        Operator(path="test", config={"test": "bar"})

    with pytest.raises(JuteCoreError):
        Operator()


def test_operator_spark_submit_raise():
    with pytest.raises(JuteCoreError):
        SparkSubmitOperator(path="test", config={"test": "bar"})

    with pytest.raises(JuteCoreError):
        SparkSubmitOperator()
