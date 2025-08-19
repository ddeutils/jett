import pytest
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructType,
)

from jett.engine.spark.utils import (
    extract_cols_selectable,
    extract_cols_without_array,
)

from .utils import field


@pytest.fixture(scope="module")
def schema() -> StructType:
    return StructType(
        [
            field("_id", IntegerType(), False),
            field("name", StringType(), True),
            field(
                "address",
                StructType(
                    [
                        field("street", StringType(), True),
                        field("city", StringType(), True),
                        field("zip_code", IntegerType(), True),
                        field("distance", ArrayType(DoubleType(), True), True),
                        field(
                            "locations",
                            ArrayType(
                                StructType(
                                    [
                                        field("latitude", DoubleType(), True),
                                        field("longitude", DoubleType(), True),
                                        field(
                                            "details",
                                            StructType(
                                                [
                                                    field(
                                                        "elevation",
                                                        DoubleType(),
                                                        True,
                                                    ),
                                                    field(
                                                        "timezone",
                                                        StringType(),
                                                        True,
                                                    ),
                                                ]
                                            ),
                                        ),
                                    ]
                                )
                            ),
                        ),
                    ]
                ),
            ),
            field(
                "items",
                ArrayType(
                    StructType(
                        [
                            field("ps5", IntegerType(), True),
                            field("nintendo_switch", IntegerType(), True),
                            field("pc", IntegerType(), True),
                        ]
                    )
                ),
                True,
            ),
            field(
                "favourite_restaurant",
                StructType(
                    [
                        field("name", StringType(), True),
                        field(
                            "menu",
                            StructType(
                                [
                                    field("name", StringType(), True),
                                    field("price", IntegerType(), True),
                                ]
                            ),
                            True,
                        ),
                    ]
                ),
                True,
            ),
        ]
    )


def test_extract_cols_selectable(schema):
    assert extract_cols_selectable(schema) == [
        "_id",
        "name",
        "address",
        "address.street",
        "address.city",
        "address.zip_code",
        "address.distance",
        "address.distance[x]",
        "address.locations",
        "address.locations[x]",
        "address.locations[x].latitude",
        "address.locations[x].longitude",
        "address.locations[x].details",
        "address.locations[x].details.elevation",
        "address.locations[x].details.timezone",
        "items",
        "items[x]",
        "items[x].ps5",
        "items[x].nintendo_switch",
        "items[x].pc",
        "favourite_restaurant",
        "favourite_restaurant.name",
        "favourite_restaurant.menu",
        "favourite_restaurant.menu.name",
        "favourite_restaurant.menu.price",
    ]


def test_extract_cols_without_array(schema):
    assert extract_cols_without_array(schema) == [
        "_id",
        "name",
        "address.street",
        "address.city",
        "address.zip_code",
        "address.distance",
        "address.locations",
        "items",
        "favourite_restaurant.name",
        "favourite_restaurant.menu.name",
        "favourite_restaurant.menu.price",
    ]
