from __future__ import annotations

from typing import Annotated, Any, Literal, Union

from pyarrow import Table
from pydantic import Field

from jett.__types import DictData
from jett.models import MetricOperatorOrder

from .__abc import BaseArrowTransform
from .functions import (
    RenameSnakeCase,
)

# GropTransform = Annotated[
#     Union[...],
#     Field(
#         discriminator="op",
#         description="A transform that allow to use in the group operator.",
#     ),
# ]
#
#
# class Group(BaseArrowTransform):
#     op: Literal["group"] = Field(description="A group operator type.")
#     transforms: list[GropTransform] = Field(description=("A "))
#
#     def apply(
#         self,
#         df: Table,
#         engine: DictData,
#         metric: MetricOperatorOrder,
#         **kwargs,
#     ) -> Table: ...
#
#     def handle_apply(
#         self,
#         df: Any,
#         context: DictData,
#         engine: DictData,
#         **kwargs,
#     ) -> Any: ...


Transform = Annotated[
    Union[
        RenameSnakeCase,
        # Group,
    ],
    Field(
        discriminator="op",
        description="A transform registry.",
    ),
]
