from collections.abc import Iterator

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, expr, udf
from pyspark.sql.types import StringType


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    spark = (
        SparkSession.builder.appName("test_spark_udf").master("local[*]")
        # .config("spark.driver.memory", "1G")
        # .config("spark.driver.cores", 1)
        .getOrCreate()
    )

    yield spark

    spark.stop()


@pytest.fixture(scope="module")
def df(spark: SparkSession) -> DataFrame:
    data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
    df = spark.createDataFrame(data, schema=["id", "name"])
    df.createTempView("people")
    df.show()
    return df


def to_upper(name):
    return name.upper()


def test_spark_udf(spark, df):
    spark.udf.register("to_upper_sql", to_upper, returnType=StringType())
    to_upper_udf = udf(to_upper, StringType())

    df.withColumn("new", to_upper_udf(col("name"))).show()
    df.withColumn("new", expr("to_upper_sql(name)")).show()


# @pytest.mark.skip()
def test_spark_udf_sql(spark, df):
    spark.udf.register("to_upper_sql", to_upper, returnType=StringType())
    result = spark.sql(
        "SELECT id, to_upper_sql(name) AS name_upper FROM people"
    )
    result.show(1, truncate=False)
