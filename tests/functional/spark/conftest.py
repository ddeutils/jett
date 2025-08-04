import logging

import pytest

from ...utils import SPARK_DISABLE

logger = logging.getLogger("jute")


@pytest.fixture(scope="package", autouse=True)
def alert_spark():
    logger.info(f"⚗️ Spark enable: {not SPARK_DISABLE}")
