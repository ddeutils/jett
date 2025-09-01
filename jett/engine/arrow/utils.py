from pyarrow import ListType, StructType


def extract_cols_without_array(schema: StructType) -> list[str]:
    """Extract selectable columns without array type.

        It returns only list of selectable columns that are not nested array
    type return only root array column name.

    Args:
        schema (StructType):

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


def extract_cols_selectable(schema: StructType, prefix: str = "") -> list[str]:
    """Extracts all selectable columns of given schema, support all top level
    column and nested column and array column.

    Returns:
        list[str]: All cols like:
            ["c1", "c2.f1", "c2.f2", "c3"]

    Examples:
        Input:
        >>> from pyspark.sql.types import StringType, IntegerType, DoubleType
        >>> StructType(
        ...     [
        ...         StructField("texts", ArrayType(StringType())),
        ...         StructField("items", ArrayType(StructType(
        ...             [
        ...                 StructField("name", StringType()),
        ...                 StructField("price", IntegerType()),
        ...                 StructField("detail", ArrayType(StructType(
        ...                     [
        ...                         StructField("field1", StringType()),
        ...                         StructField("field2", DoubleType()),
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
    for field in schema:
        rs.append(prefix + field.name)
        field_type = field.dataType
        if isinstance(field_type, StructType):
            rs.extend(
                extract_cols_selectable(field_type, f"{prefix}{field.name}.")
            )
        elif isinstance(field_type, ListType):
            rs.append(prefix + field.name + "[x]")
            if isinstance(field_type.elementType, StructType):
                rs.extend(
                    extract_cols_selectable(
                        field_type.elementType, f"{prefix}{field.name}[x]."
                    )
                )
    return rs
