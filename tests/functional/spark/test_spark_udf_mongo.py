import json
import random
import time
from collections.abc import Iterator

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col, expr, from_json, schema_of_json

from ...utils import SPARK_DISABLE


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    """Create and configure Spark session for MongoDB JSON processing."""
    session: SparkSession = (
        SparkSession.builder.appName("test_spark_mongo_udf")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .getOrCreate()
    )

    yield session

    session.stop()


@pytest.fixture(scope="module", autouse=True)
def register_udf(spark: SparkSession):
    """Register the MongoDB JSON cleaner as a UDF."""
    from jett.engine.spark.udf import clean_mongo_json_udf

    return clean_mongo_json_udf(spark=spark)


# @pytest.mark.skipif(SPARK_DISABLE, reason="Spark testing does not enable.")
def test_spark_udf_mongo(spark: SparkSession, register_udf):
    """Example of basic UDF usage with sample MongoDB JSON data."""
    sample_data = [
        (
            "1",
            (
                '{"_id": {"$oid": "507f1f77bcf86cd799439011"}, '
                '"name": "John", '
                '"age": {"$numberLong": "30"}}'
            ),
        ),
        (
            "2",
            '{"_id": {"$oid": "507f1f77bcf86cd799439012"}, "name": "Jane", "created": {"$date": "2023-01-01T00:00:00.000Z"}}',
        ),
        (
            "3",
            '{"_id": {"$oid": "507f1f77bcf86cd799439013"}, "price": {"$numberDecimal": "99.99"}, "tags": ["tag1", "tag2"]}',
        ),
        (
            "4",
            '{"_id": {"$oid": "507f1f77bcf86cd799439014"}, "nested": {"user": {"id": {"$oid": "507f1f77bcf86cd799439015"}, "score": {"$numberDouble": "85.5"}}}}',
        ),
    ]

    # Create DataFrame
    df = spark.createDataFrame(sample_data, ["id", "mongo_json"])
    df.show()
    df_cleaned = df.withColumn("cleaned_json", register_udf(col("mongo_json")))
    df_cleaned.show(1, truncate=False)
    rows: list[Row] = df_cleaned.collect()
    assert {
        row.asDict()["id"]: json.loads(row.asDict()["cleaned_json"])
        for row in rows
    } == {
        "1": {"_id": "507f1f77bcf86cd799439011", "name": "John", "age": 30},
        "2": {
            "_id": "507f1f77bcf86cd799439012",
            "name": "Jane",
            "created": "2023-01-01T00:00:00+00:00",
        },
        "3": {
            "_id": "507f1f77bcf86cd799439013",
            "price": "99.99",
            "tags": ["tag1", "tag2"],
        },
        "4": {
            "_id": "507f1f77bcf86cd799439014",
            "nested": {
                "user": {"id": "507f1f77bcf86cd799439015", "score": 85.5}
            },
        },
    }


@pytest.mark.skipif(SPARK_DISABLE, reason="Spark testing does not enable.")
def test_spark_udf_mongo_sql(spark, register_udf):
    """Example using SQL with registered UDF."""

    # Create temporary view
    sample_data = [
        ("1", '{"_id": {"$oid": "507f1f77bcf86cd799439011"}, "name": "John"}'),
        ("2", '{"_id": {"$oid": "507f1f77bcf86cd799439012"}, "name": "Jane"}'),
    ]

    df = spark.createDataFrame(sample_data, ["id", "mongo_json"])
    df.show(truncate=False)
    df.createOrReplaceTempView("mongo_data")
    df.withColumn("cleaned_json", expr("clean_mongo_json(mongo_json)"))
    result = spark.sql(
        """
        SELECT
            id
            , mongo_json as original_json
            , clean_mongo_json(mongo_json) as cleaned_json
        FROM mongo_data
    """
    )
    result.show(truncate=False)


@pytest.mark.skipif(SPARK_DISABLE, reason="Spark testing does not enable.")
def test_spark_udf_mongo_complex_processing(spark, register_udf):
    """Example of complex processing with nested JSON structures."""

    # Complex nested MongoDB JSON
    complex_data = [
        (
            "1",
            """
        {
            "user": {
                "id": {"$oid": "507f1f77bcf86cd799439011"},
                "profile": {
                    "name": "John Doe",
                    "age": {"$numberLong": "30"},
                    "created": {"$date": "2023-01-01T00:00:00.000Z"}
                },
                "orders": [
                    {
                        "order_id": {"$oid": "507f1f77bcf86cd799439016"},
                        "amount": {"$numberDecimal": "99.99"},
                        "items": ["item1", "item2"]
                    },
                    {
                        "order_id": {"$oid": "507f1f77bcf86cd799439017"},
                        "amount": {"$numberDecimal": "149.99"},
                        "items": ["item3"]
                    }
                ]
            }
        }
        """,
        ),
    ]

    df = spark.createDataFrame(complex_data, ["id", "mongo_json"])
    df_cleaned = df.withColumn("cleaned_json", register_udf(col("mongo_json")))

    # Parse the cleaned JSON to extract specific fields
    # First, infer schema from the cleaned JSON
    sample_cleaned = df_cleaned.select("cleaned_json").first()[0]
    json_schema = schema_of_json(sample_cleaned)

    # Parse the cleaned JSON
    df_parsed = df_cleaned.withColumn(
        "parsed_data", from_json(col("cleaned_json"), json_schema)
    )
    df_parsed.show(truncate=False)


@pytest.mark.skipif(SPARK_DISABLE, reason="Spark testing does not enable.")
def test_spark_udf_mongo_performance_optimization(spark, register_udf):
    """Example showing performance optimization techniques."""

    def generate_mongo_json():
        """Generate sample MongoDB JSON for testing."""
        oid = f"507f1f77bcf86cd7994390{random.randint(10, 99)}"
        return f'{{"_id": {{"$oid": "{oid}"}}, "value": {{"$numberLong": "{random.randint(1, 1000)}"}}}}'

    # Generate test data
    test_data = [(str(i), generate_mongo_json()) for i in range(1000)]
    df = spark.createDataFrame(test_data, ["id", "mongo_json"])

    # Cache the DataFrame for better performance
    df.cache()

    print(f"Processing {df.count()} records...")

    # Apply UDF with performance monitoring
    start_time = time.time()

    df_cleaned = df.withColumn("cleaned_json", register_udf(col("mongo_json")))

    count = df_cleaned.count()
    end_time = time.time()

    print(f"Processed {count} records in {end_time - start_time:.2f} seconds")
    print(
        f"Average time per record: {(end_time - start_time) / count * 1000:.2f} milliseconds"
    )
    df_cleaned.show(5, truncate=False)


@pytest.mark.skipif(SPARK_DISABLE, reason="Spark testing does not enable.")
def test_spark_udf_mongo_error_handling(spark, register_udf):
    """Example showing error handling with invalid JSON data."""

    # Data with some invalid JSON
    error_data = [
        ("1", '{"_id": {"$oid": "507f1f77bcf86cd799439011"}, "name": "valid"}'),
        ("2", '{"invalid": json}'),  # Invalid JSON
        ("3", ""),  # Empty string
        (
            "4",
            '{"_id": {"$oid": "invalid_oid"}, "name": "invalid_oid"}',
        ),  # Invalid ObjectId
    ]

    df = spark.createDataFrame(error_data, ["id", "mongo_json"])

    try:
        df_cleaned = df.withColumn(
            "cleaned_json", register_udf(col("mongo_json"))
        )
        df_cleaned.show(truncate=False)
    except Exception as e:
        print(f"Error processing data: {e}")
