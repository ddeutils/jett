from pathlib import Path
from typing import Any

import pytest
import yaml

from detool.engine.__abc import BaseEngine
from detool.utils import (
    handle_command,
    is_optional_engine,
    load_yaml,
    sort_non_sensitive_str,
    to_snake_case,
)

from ..utils import filter_updated_and_created


@pytest.fixture(scope="module")
def mock_yaml_file(root_path: Path):
    mock_file: Path = root_path / "assets/detool.mock.utils.yaml"
    with mock_file.open(mode="w") as f:
        yaml.dump({"foo": "bar"}, f)

    yield mock_file

    mock_file.unlink(missing_ok=True)


def test_utils_load_yaml_not_exists(test_path: Path):
    data = load_yaml(test_path / "assets/detool.not-exists.yaml")
    assert data == {}


def test_utils_load_yaml(mock_yaml_file: Path):
    data = load_yaml(mock_yaml_file)
    assert filter_updated_and_created(data) == {
        "foo": "bar",
        "parent_dir": mock_yaml_file.parent,
    }

    data = load_yaml(str(mock_yaml_file.resolve()))
    assert filter_updated_and_created(data) == {
        "foo": "bar",
        "parent_dir": mock_yaml_file.parent,
    }


def test_to_snake_case():
    assert to_snake_case("columnName") == "column_name"
    assert to_snake_case("ColumnID") == "column_id"
    assert to_snake_case("demoIDForm") == "demo_id_form"
    assert to_snake_case("IDNameOnRMF") == "id_name_on_rmf"


def mock_func(df: Any, *, engine: BaseEngine | None = None):  # pragma: no cov
    return


def mock_func_not_exits(df: Any):  # pragma: no cov
    return


def test_is_optional_engine():
    assert is_optional_engine(mock_func)
    assert not is_optional_engine(mock_func_not_exits)


def test_handle_command():
    for o in handle_command("git branch -a"):
        print(o)


def test_sort_list_str_non_sensitive():
    assert list(sort_non_sensitive_str(["s b", "Fo/o", "G$11"])) == [
        "Fo/o",
        "G$11",
        "s b",
    ]
