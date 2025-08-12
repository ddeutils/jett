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


def test_polars_explode():
    from string import ascii_lowercase

    df = pl.DataFrame(
        {
            "letters": ["a", "a", "b", "c", "d", "e"],
            "numbers": [[1, 1], [2, 3], [4, 5], [6, 7], [None], []],
        }
    )

    print(df)
    rs = df.with_columns(
        pl.col("numbers").list.to_struct(
            fields=lambda idx: f"numbers_{ascii_lowercase[idx]}",
            n_field_strategy="max_width",
            _eager=True,
        )
    ).unnest("numbers")
    print(rs)

    max_width = df["numbers"].list.len().max()
    rs = df.select(
        pl.exclude("numbers"),
        *[
            pl.col("numbers")
            .list.get(x, null_on_oob=True)
            .alias(f"numbers_{ascii_lowercase[x]}")
            for x in range(max_width)
        ],
    )
    print(rs)

    rs = (
        df.with_row_index()
        .explode("numbers")
        .with_columns(
            on=pl.format("numbers_{}", pl.int_range(pl.len()).over("index") + 1)
        )
        .pivot(on="on", index=["index", "letters"])
        .drop("index")
    )
    print(rs)
