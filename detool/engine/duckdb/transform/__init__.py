from typing import Annotated, Union

from pydantic import Field

from .functions import (
    DropColumns,
    ExcludeColumns,
    RenameColumns,
    RenameSnakeCase,
    SQLExecute,
)

Transform = Annotated[
    Union[
        SQLExecute,
        DropColumns,
        ExcludeColumns,
        RenameColumns,
        RenameSnakeCase,
    ],
    Field(discriminator="op"),
]
ListTransform = list[Transform]
