from collections.abc import Iterator
from pathlib import Path

import polars as pl
import pytest
from polars import DataFrame, Field, Schema, Struct
from pyiceberg.catalog import Catalog, Table, load_catalog


@pytest.fixture(scope="module")
def catalog(test_path: Path) -> Iterator[Catalog]:
    warehouse_path = test_path / "tmp/warehouse"
    if not warehouse_path.exists():
        warehouse_path.mkdir(parents=True)

    c: Catalog = load_catalog(
        "default",
        **{
            "type": "sql",
            "uri": f"sqlite:///{warehouse_path.absolute()}/pyiceberg_catalog.db",
            "warehouse": f"file://{warehouse_path.absolute()}",
        },
    )
    print(type(c))

    # NOTE: from pyiceberg.catalog.sql import SqlCatalog
    yield c


@pytest.fixture(scope="module")
def schema():
    return Schema(
        schema=Struct(
            [
                Field("id", pl.Int64()),
                Field("name", pl.String()),
            ]
        )
    )


@pytest.fixture(scope="module")
def df(schema):
    df = pl.DataFrame(
        data=[
            (1, "tom"),
            (2, "foo"),
        ],
        schema=schema,
        orient="row",
    )
    print(df)
    yield df


def test_polars_iceberg_create_namespace(catalog):
    catalog.create_namespace("default")


def test_polars_iceberg_create_table(catalog, df: DataFrame):
    try:
        _: Table = catalog.create_table(
            "default.mock_data", schema=df.to_arrow().schema
        )
    except Exception:
        pass
    table = catalog.load_table("default.mock_data")
    print(type(table))
    print(table)
    print(df)
    df.write_iceberg(table, mode="append")


def test_polars_iceberg_load_table(catalog):
    table = catalog.load_table("default.mock_data")
    print(table.current_snapshot())
