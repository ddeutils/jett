from collections.abc import Iterator

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from ...utils import SPARK_DISABLE


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    spark = SparkSession.builder.appName("test_spark_session").getOrCreate()

    yield spark

    spark.stop()


@pytest.fixture(scope="module")
def df(spark: SparkSession) -> DataFrame:
    data = [
        ("Alice", 1, "New York"),
        ("Bob", 2, "London"),
        ("Charlie", 3, "Paris"),
    ]
    schema = StructType(
        [
            StructField("Name", StringType(), True),
            StructField("ID", IntegerType(), True),
            StructField("City", StringType(), True),
        ]
    )
    df = spark.createDataFrame(data, schema)
    df.show()
    return df


@pytest.mark.skipif(SPARK_DISABLE, reason="Spark testing does not enable.")
def test_get_session(spark: SparkSession):
    print(spark.version)
    print(spark.catalog.listTables())


@pytest.mark.skipif(SPARK_DISABLE, reason="Spark testing does not enable.")
def test_create_sample_data_frame(spark: SparkSession):
    data = [
        ("Alice", 1, "New York"),
        ("Bob", 2, "London"),
        ("Charlie", 3, "Paris"),
    ]
    schema = StructType(
        [
            StructField("Name", StringType(), True),
            StructField("ID", IntegerType(), True),
            StructField("City", StringType(), True),
        ]
    )
    df = spark.createDataFrame(data, schema)
    df.show()
    df.cache()
    assert df.count() == 3
