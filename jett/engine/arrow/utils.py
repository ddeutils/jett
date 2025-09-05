from __future__ import annotations

from pyarrow import Schema, StructType
from pyarrow.types import is_fixed_size_list, is_large_list, is_list, is_struct


def schema2dict(
    schema: Schema, sorted_by_name: bool = False
) -> list[dict[str, str]]:
    """Convert Arrow Schema object to dict of field mapping that include keys,
    `name` and `dtype`.

    Args:
        schema (Schema): A Arrow Schema object.
        sorted_by_name (bool, default False): A flag that will sort returning
            list of field mapping by name if it set be True.
    """
    rs: list[dict[str, str]] = [
        {"name": f.name, "dtype": str(f.dtype)} for f in schema
    ]
    return sorted(rs, key=lambda x: x["name"]) if sorted_by_name else rs


def extract_cols_without_array(schema: Schema) -> list[str]:
    """Extract selectable columns without array type.

        It returns only list of selectable columns that are not nested array
    type return only root array column name.

    Args:
        schema (Schema): A Arrow Schema object.

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
        not_parent: bool = True
        for fc in final_selectable_cols:
            if fc != c and fc.startswith(f"{c}."):
                not_parent: bool = False

        if not_parent:
            rs.append(c)
    return rs


def extract_cols_selectable(
    schema: Schema | StructType,
    *,
    prefix: str = "",
) -> list[str]:
    """
    Extracts all selectable columns of a given pyarrow schema,
    including nested struct fields and list (array) fields.

    Args:
        schema (pa.Schema): The pyarrow schema.
        prefix (str, optional): Internal prefix for recursion.

    Returns:
        list[str]: List of selectable column paths.

    Example:
        >>> import pyarrow as pa
        >>> _schema = pa.schema([
        ...     pa.field("texts", pa.list_(pa.string())),
        ...     pa.field("items", pa.list_(
        ...         pa.struct([
        ...             pa.field("name", pa.string()),
        ...             pa.field("price", pa.int32()),
        ...             pa.field("detail", pa.list_(
        ...                 pa.struct([
        ...                     pa.field("field1", pa.string()),
        ...                     pa.field("field2", pa.float64()),
        ...                 ])
        ...             ))
        ...         ])
        ...     ))
        ... ])
        >>> extract_cols_selectable(_schema)
        [
            'texts',
            'texts[x]',
            'items',
            'items[x]',
            'items[x].name',
            'items[x].price',
            'items[x].detail',
            'items[x].detail[x]',
            'items[x].detail[x].field1',
            'items[x].detail[x].field2'
        ]
    """
    rs: list[str] = []
    for field in schema:
        field_name = prefix + field.name
        rs.append(field_name)

        field_type = field.type
        if is_struct(field_type):
            rs.extend(
                extract_cols_selectable(field_type, prefix=f"{field_name}.")
            )

        # NOTE: Array types (list, large_list, fixed_size_list)
        elif (
            is_list(field_type)
            or is_large_list(field_type)
            or is_fixed_size_list(field_type)
        ):
            rs.append(field_name + "[x]")
            element_type = field_type.value_type
            if is_struct(element_type):
                rs.extend(
                    extract_cols_selectable(
                        element_type, prefix=f"{field_name}[x]."
                    )
                )

    return rs
