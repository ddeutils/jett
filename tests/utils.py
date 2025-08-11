import logging
import os
from functools import partial
from pathlib import Path
from typing import Any
from urllib.request import urlretrieve

from pyspark.sql import SparkSession

logger = logging.getLogger("detool")

SPARK_DISABLE: bool = not bool(int(os.getenv("TOOL__TEST__SPARK_ENABLE", "0")))


def filter_keys(
    value: dict[str, Any], *, keys: tuple[str, ...]
) -> dict[str, Any]:
    return {k: value[k] for k in value if k not in keys}


filter_updated_and_created: partial[dict[str, Any]] = partial(
    filter_keys, keys=("created_at", "updated_at")
)


def get_spark_session(test_path: Path | None = None) -> SparkSession:
    """Get Spark Session for testing."""
    jars_path: str = f"{(test_path or Path('.')).absolute()}/.jars"

    # jars_url = [
    #     "https://line-objects-internal.com/dap-artifact/dap-spark-udf/release/production/v1.0.0/dap-spark-udf-1.0.0.jar",
    # ]
    jars_url = []
    jars_packages = [
        "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
        "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3",
        "org.apache.iceberg:iceberg-hive-runtime:1.4.3",
        "com.mysql:mysql-connector-j:8.4.0",
        "org.postgresql:postgresql:42.6.2",
        "com.amazonaws:aws-java-sdk-bundle:1.11.901",
        "org.apache.hadoop:hadoop-aws:3.3.4",
    ]

    jars = []
    logger.debug("Download jar files")
    logger.debug(f"jars url: {jars_url}")

    if jars_url:
        if not os.path.exists(jars_path):
            os.makedirs(jars_path)

    for url in jars_url:
        filename = url.split("/")[-1]
        jar_filepath = f"{jars_path}/{filename}"
        if not os.path.exists(jar_filepath):
            urlretrieve(url, jar_filepath)
        jars.append(jar_filepath)
    logger.debug("jars have been downloaded")
    logger.debug("init spark session")
    # logger.debug("spark.jars=%s", jars)
    logger.debug("spark.jars.packages=%s", jars_packages)

    # NOTE: force unittest uses only local dir, otherwise, spark will fail due
    # to it cannot mount with external storage.
    spark = (
        SparkSession.builder.appName("Session For Unittest")
        .master("local[*]")
        # .config("spark.jars", ",".join(jars))
        .config("spark.jars.packages", ",".join(jars_packages))
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog"
        )
        .config("spark.sql.catalog.local.type", "hadoop")
        .config(
            "spark.sql.catalog.local.warehouse",
            f"{test_path.absolute()}/spark-warehouse",
        )
        .config("spark.sql.defaultCatalog", "local")
        .getOrCreate()
    )

    return spark
