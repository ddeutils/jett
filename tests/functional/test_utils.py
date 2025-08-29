import os
from collections.abc import Iterator
from pathlib import Path
from textwrap import dedent
from typing import Any

import pytest
import yaml

from jett.engine.__abc import BaseEngine
from jett.utils import (
    handle_command,
    is_optional_engine,
    load_yaml,
    sort_non_sensitive_str,
    substitute_env_vars,
    to_snake_case,
)

from ..utils import filter_updated_and_created


@pytest.fixture(scope="module")
def mock_yaml_file(root_path: Path):
    mock_file: Path = root_path / "assets/jett.mock.utils.yaml"
    with mock_file.open(mode="w") as f:
        yaml.dump({"foo": "bar"}, f)

    yield mock_file

    mock_file.unlink(missing_ok=True)


def test_utils_load_yaml_not_exists(test_path: Path):
    data = load_yaml(test_path / "assets/jett.not-exists.yaml")
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


@pytest.fixture(scope="function")
def set_env_var():
    os.environ["TEST_SECRET_DATA"] = "bar"
    yield
    del os.environ["TEST_SECRET_DATA"]


@pytest.fixture(scope="module")
def mock_config() -> Iterator[Path]:
    config_file = Path("./test_config.yaml")
    with open(config_file, "w") as f:
        f.write(
            dedent(
                """
            conf:
               secret: "${{ TEST_SECRET_DATA:default }}"
               name: foo
               port: "${{ PORT:8080 }}"
               flag: on
               values:
                  - on: test
                  - on: staging
            """.lstrip(
                    "\n"
                )
            )
        )

    yield config_file

    config_file.unlink(missing_ok=True)


def test_substitute_env_vars(set_env_var, mock_config):
    assert substitute_env_vars(load_yaml(mock_config, add_info=False)) == {
        "conf": {
            "secret": "bar",
            "name": "foo",
            "port": "8080",
            "flag": True,
            "values": [{True: "test"}, {True: "staging"}],
        }
    }


def test_substitute_env_vars_default(mock_config):
    assert substitute_env_vars(load_yaml(mock_config, add_info=False)) == {
        "conf": {
            "secret": "default",
            "name": "foo",
            "port": "8080",
            "flag": True,
            "values": [{True: "test"}, {True: "staging"}],
        }
    }
