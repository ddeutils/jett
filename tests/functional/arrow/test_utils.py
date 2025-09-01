import pytest
from pyarrow import (
    Schema,
    Table,
    field,
    float64,
    int64,
    schema,
    string,
    struct,
)


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
