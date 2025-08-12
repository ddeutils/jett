import re

import polars as pl
from polars import Array, Expr, Field, List, Schema, Struct


def schema2struct(schema: Schema) -> Struct:
    """Convert Schema object to Struct."""
    return Struct([Field(name, dtype) for name, dtype in schema.items()])


def extract_cols_selectable(
    schema: Schema | Struct, prefix: str = ""
) -> list[str]:
    """Extracts all selectable columns of given schema, support all top level
    column and nested column and array column.

    Args:
        schema (Schema | Struct): A Polars Schema object.
        prefix (str, default ''): A prefix value.

    Returns:
        list[str]: All cols like:
            ["c1", "c2.f1", "c2.f2", "c3"]

    Examples:
        Input:
        >>> from polars import Field, String, Int64, Float64
        >>> Struct(
        ...     [
        ...         Field("texts", Array(String())),
        ...         Field("items", Array(Struct(
        ...             [
        ...                 Field("name", String()),
        ...                 Field("price", Int64()),
        ...                 Field("detail", Array(Struct(
        ...                     [
        ...                         Field("field1", String()),
        ...                         Field("field2", Float64()),
        ...                     ]
        ...                 )))
        ...             ]
        ...         )))
        ...     ]
        ... )

        Output:
        >>> [
        ...     'texts',
        ...     'texts[x]',
        ...     'items',
        ...     'items[x]',
        ...     'items[x].name',
        ...     'items[x].price',
        ...     'items[x].detail',
        ...     'items[x].detail[x]',
        ...     'items[x].detail[x].field1',
        ...     'items[x].detail[x].field2',
        ... ]
    """
    rs: list[str] = []
    struct: Struct = (
        schema2struct(schema) if isinstance(schema, Schema) else schema
    )
    for field in struct.fields:
        rs.append(prefix + field.name)
        if isinstance(field.dtype, Struct):
            rs.extend(
                extract_cols_selectable(field.dtype, f"{prefix}{field.name}.")
            )
        elif isinstance(field.dtype, (Array, List)):
            rs.append(f"{prefix}{field.name}[x]")
            if isinstance(field.dtype.inner, Struct):
                rs.extend(
                    extract_cols_selectable(
                        field.dtype.inner, f"{prefix}{field.name}[x]."
                    )
                )
    return rs


def extract_cols_without_array(schema: Schema) -> list[str]:
    """Extract selectable columns without array type.

        It returns only list of selectable columns that are not nested array
    type return only root array column name.

    Args:
        schema (Schema):

    Returns:
        list[str]: A list of column name that extract by selectable without
            array type.
    """
    selectable_cols: list[str] = extract_cols_selectable(schema=schema)
    nested_array_cols: list[str] = [c for c in selectable_cols if "[x]" in c]
    final_selectable_cols: list[str] = [
        c for c in selectable_cols if c not in nested_array_cols
    ]

    rs: list[str] = []
    for c in final_selectable_cols:
        is_not_parent_column: bool = True
        for _column in final_selectable_cols:
            if _column != c and _column.startswith(f"{c}."):
                is_not_parent_column: bool = False

        if is_not_parent_column:
            rs.append(c)
    return rs


def col_path(path: str) -> Expr:
    """Convert a dotted path notation to polars column expression supports also
    list access

    Examples:
        >>> col_path('a[5].b')

    References:
        - https://gist.github.com/ophiry/78e6e04a8fde01e58ee289febf3bc4cc
    """
    parsed_path = re.findall(r"\.(\w+)|\[(\d+)]", f".{path}")

    expr = pl.col(parsed_path[0][0])
    for field, index in parsed_path[1:]:
        if field:
            expr = expr.struct.field(field)
        elif index:
            expr = expr.list.get(int(index))
    return expr
