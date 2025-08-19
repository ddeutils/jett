import pytest
from polars import Field, Float64, Int64, List, Schema, String, Struct

from jett.engine.polars.utils import (
    extract_cols_selectable,
    extract_cols_without_array,
    schema2struct,
)


@pytest.fixture(scope="module")
def struct():
    return Struct(
        [
            Field("_id", Int64()),
            Field("name", String()),
            Field(
                "address",
                Struct(
                    [
                        Field("street", String()),
                        Field("city", String()),
                        Field("zip_code", Int64()),
                        Field("distance", List(Float64())),
                        Field(
                            "locations",
                            List(
                                Struct(
                                    [
                                        Field("latitude", Float64()),
                                        Field("longitude", Float64()),
                                        Field(
                                            "details",
                                            Struct(
                                                [
                                                    Field(
                                                        "elevation", Float64()
                                                    ),
                                                    Field("timezone", String()),
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
            Field(
                "items",
                List(
                    Struct(
                        [
                            Field("ps5", Int64()),
                            Field("nintendo_switch", Int64()),
                            Field("pc", Int64()),
                        ]
                    )
                ),
            ),
            Field(
                "favourite_restaurant",
                Struct(
                    [
                        Field("name", String()),
                        Field(
                            "menu",
                            Struct(
                                [
                                    Field("name", String()),
                                    Field("price", Int64()),
                                ]
                            ),
                        ),
                    ]
                ),
            ),
        ]
    )


@pytest.fixture(scope="module")
def schema(struct) -> Schema:
    return Schema(struct)


def test_schema2struct(schema, struct):
    assert schema2struct(schema) == struct


def test_extract_cols_selectable(schema: Schema):
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
