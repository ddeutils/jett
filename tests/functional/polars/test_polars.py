from pathlib import Path
from typing import get_type_hints

import polars as pl

from detool.engine.polars.transform.functions import RenameColumns


def test_polars():
    c = pl.col("test").cast(pl.String)
    print(type(c))


def test_polars_check_type():
    print(get_type_hints(RenameColumns.apply))
    print(get_type_hints(test_polars))


def test_polars_schema_type():
    from polars import DataFrame, Field, Int64, List, Schema, String, Struct

    schema = Schema(
        Struct(
            [
                Field("id", Int64),
                Field("timestamp", String),
                Field(
                    "user",
                    Struct([Field("name", String), Field("email", String)]),
                ),
                Field(
                    "items",
                    List(Struct({"item_id": String, "quantity": Int64})),
                ),
            ]
        )
    )
    print(schema)

    df: DataFrame = pl.DataFrame(data=[], schema=schema)
    print(df)


def test_polars_select(test_path: Path):
    df = pl.read_ndjson(test_path.parent / "assets/data/explode-customer.json")
    print(df)

    # print(df.select(pl.col("user.name")))  # Error
    print(df.select(pl.col("user").struct.field("name")))

    print(df.select(pl.col("user").struct["*"]))
