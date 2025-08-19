import os
from collections.abc import Iterator
from pathlib import Path
from textwrap import dedent

import pytest

from jett.conf import substitute_env_vars
from jett.utils import load_yaml


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
