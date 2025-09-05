from __future__ import annotations

import pyarrow as pa
import pytest
from pyarrow import (
    Field,
    Schema,
    StructType,
    Table,
    field,
    float64,
    int64,
    schema,
    string,
    struct,
)

from jett.engine.arrow.utils import extract_cols_selectable


@pytest.fixture(scope="module")
def mock_schema() -> Schema:
    return schema(
        [
            field("id", int64()),
            field("name", string()),
            field("income", float64()),
            field(
                "data",
                struct(
                    [
                        field("a", string()),
                        field("b", string()),
                    ]
                ),
            ),
        ]
    )


def test_arrow_table(mock_schema):
    tbl: Table = Table.from_pylist([], schema=mock_schema)
    print(tbl)
    print(tbl.schema)

    tbl: Table = Table.from_pylist(
        [
            {
                "id": 1,
                "name": "foo",
                "income": 100.15,
                "data": {"a": "test_a", "b": "test_b"},
            }
        ],
        schema=mock_schema,
    )
    print(tbl)
    print(tbl.schema)


def test_convert_schema_to_struct(mock_schema):
    assert isinstance(mock_schema, Schema)
    st = struct(list(mock_schema))

    assert isinstance(st, StructType)
    for f in st:
        assert isinstance(f, Field)
        print(f.name, f": {f.type}", f"( {type(f)} )")


def test_extract_cols_selectable():
    _schema: Schema = schema(
        [
            field(
                "user",
                struct(
                    [
                        field("id", pa.int64()),
                        field("name", pa.string()),
                        field("emails", pa.list_(pa.string())),
                        field(
                            "profile",
                            struct(
                                [
                                    field("active", pa.bool_()),
                                    field("created_at", pa.timestamp("ms")),
                                    field("scores", pa.list_(pa.float64(), 3)),
                                ]
                            ),
                        ),
                    ]
                ),
            ),
            field(
                "orders",
                pa.list_(
                    struct(
                        [
                            field("order_id", pa.string()),
                            field(
                                "items",
                                pa.list_(
                                    struct(
                                        [
                                            field("sku", pa.string()),
                                            field(
                                                "price", pa.decimal128(10, 2)
                                            ),
                                            field(
                                                "tags",
                                                pa.large_list(pa.string()),
                                            ),
                                        ]
                                    )
                                ),
                            ),
                        ]
                    )
                ),
            ),
            field("metadata", pa.map_(pa.string(), pa.string())),
        ]
    )
    result = extract_cols_selectable(_schema)
    assert result == [
        "user",
        "user.id",
        "user.name",
        "user.emails",
        "user.emails[x]",
        "user.profile",
        "user.profile.active",
        "user.profile.created_at",
        "user.profile.scores",
        "user.profile.scores[x]",
        "orders",
        "orders[x]",
        "orders[x].order_id",
        "orders[x].items",
        "orders[x].items[x]",
        "orders[x].items[x].sku",
        "orders[x].items[x].price",
        "orders[x].items[x].tags",
        "orders[x].items[x].tags[x]",
        "metadata",
    ]


@pytest.mark.parametrize(
    "arrow_type",
    [
        pa.null(),
        pa.bool_(),
        pa.int8(),
        pa.int16(),
        pa.int32(),
        pa.int64(),
        pa.uint8(),
        pa.uint16(),
        pa.uint32(),
        pa.uint64(),
        pa.float16(),
        pa.float32(),
        pa.float64(),
        pa.string(),
        pa.large_string(),
        pa.binary(),
        pa.large_binary(),
        pa.binary(4),
        pa.decimal128(10, 2),
        pa.decimal256(20, 4),
        pa.date32(),
        pa.date64(),
        pa.timestamp("s"),
        pa.timestamp("ms"),
        pa.timestamp("us"),
        pa.timestamp("ns"),
        pa.time32("s"),
        pa.time32("ms"),
        pa.time64("us"),
        pa.time64("ns"),
        pa.duration("s"),
        pa.month_day_nano_interval(),
        pa.list_(pa.int32()),
        pa.large_list(pa.string()),
        pa.list_(pa.int8(), 3),
        pa.struct([pa.field("f1", pa.int32()), pa.field("f2", pa.string())]),
        pa.map_(pa.string(), pa.int32()),
    ],
)
def test_extract_cols_selectable_arrow_primitives(arrow_type):
    _schema = pa.schema([("col", arrow_type)])
    result = extract_cols_selectable(_schema)
    assert "col" in result

    # NOTE: If type be list/array, it should have `[x]`.
    if (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        assert "col[x]" in result

    # NOTE: If type be struct, it should span field inside.
    if pa.types.is_struct(arrow_type):
        assert any(r.startswith("col.") for r in result)

    # NOTE: If type be mapped, its `(key, item)` will be struct.
    if pa.types.is_map(arrow_type):
        assert "col[key]" not in result
