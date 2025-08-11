from typing import Annotated, Literal, Union

import polars as pl
from polars import DataFrame
from polars import IntoExpr as Column
from pydantic import Field

from ....__types import DictData
from .__abc import TYPES, BasePolarsTransform


class RenameTransform(BasePolarsTransform):
    op: Literal["rename"]
    name: str
    source: str
    type: str | None = Field(default=None)

    def apply(
        self,
        df: DataFrame,
        engine: DictData,
        **kwargs,
    ) -> tuple[str, Column]:
        select = pl.col(self.source)
        if self.type:
            select.cast(TYPES[self.type])
        return self.name, select


Transform = Annotated[Union[RenameTransform,], Field(discriminator="op")]


class Group(BasePolarsTransform):
    op: Literal["group"]
    transforms: list[Transform]

    def apply(
        self,
        df: DataFrame,
        engine: DictData,
        **kwargs,
    ) -> DataFrame:
        return df.with_columns(
            **dict(op.apply(df, engine=engine) for op in self.transforms)
        )
