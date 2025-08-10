import pytest

from detool.errors import ToolError
from detool.tools import SparkSubmitTool, Tool


def test_tool():
    op = Tool(
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

    with pytest.raises(ToolError):
        op.c.fetch(op.c.data)

    op.refresh()

    assert op.c.model.type == "duckdb"
    assert op.c.model.source.type == "local"


def test_tool_raise():
    with pytest.raises(ToolError):
        Tool(path="test", config={"test": "bar"})

    with pytest.raises(ToolError):
        Tool()


def test_tool_spark_submit_raise():
    with pytest.raises(ToolError):
        SparkSubmitTool(path="test", config={"test": "bar"})

    with pytest.raises(ToolError):
        SparkSubmitTool()
