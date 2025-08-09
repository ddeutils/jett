from typing import Annotated, Union

from pydantic import Field

from .compute_metric import CalculateMinMaxOfColumns, DetectSchemaChangeWithSink
from .cryptography import GCMDecrypt
from .functions import (
    SQL,
    CleanMongoJsonStr,
    DropColumns,
    ExplodeArrayColumn,
    Expr,
    FlattenAllExceptArray,
    JsonStrToStruct,
    RenameColumns,
    RenameSnakeCase,
    Scd2,
)
from .validation import ValidateColumnDisallowSpace

Transform = Annotated[
    Union[
        GCMDecrypt,
        Expr,
        SQL,
        DropColumns,
        RenameColumns,
        RenameSnakeCase,
        ExplodeArrayColumn,
        FlattenAllExceptArray,
        Scd2,
        CleanMongoJsonStr,
        JsonStrToStruct,
        ValidateColumnDisallowSpace,
        CalculateMinMaxOfColumns,
        DetectSchemaChangeWithSink,
    ],
    Field(
        discriminator="op",
        description="A transform registry",
    ),
]
ListTransform = list[Transform]
