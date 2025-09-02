import logging
from pathlib import Path

import pytest

logger = logging.getLogger("jett")


@pytest.fixture(scope="package", autouse=True)
def alert_unittest():
    message: str = (
        "🧪⚗️ Start Run the Unittest testcase for Jett package. This unittest "
        "include only functional testcase that do not run any integration "
        "module."
    )
    print(message)
    logger.info(message)


@pytest.fixture(scope="package")
def test_path() -> Path:
    return Path(__file__).parent.parent


@pytest.fixture(scope="package")
def root_path(test_path: Path) -> Path:
    return test_path.parent
