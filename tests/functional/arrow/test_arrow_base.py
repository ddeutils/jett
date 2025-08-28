from pyarrow import Field, Schema, Table
from pyarrow.json import read_json


def test_pa_read_json(root_path):
    table: Table = read_json(root_path / "assets/data/explode-customer.json")
    print(table)
    print(table.shape)
    print(table.schema)
    print(type(table.schema))
    schema: Schema = table.schema
    field: Field
    for field in schema:
        print(field.name, ":", field.type, f"({type(field.type)})")
