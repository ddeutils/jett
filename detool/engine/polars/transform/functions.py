import logging
from typing import Literal

from polars import DataFrame
from pydantic import Field

from ....__types import DictData
from .__abc import BasePolarsTransform
from .__models import ColumnMap

logger = logging.getLogger("detool")


class RenameColumns(BasePolarsTransform):
    """Rename Columns Transform model."""

    op: Literal["rename_columns"]
    columns: list[ColumnMap] = Field(description="A list of ColumnMap object.")
    allow_group_transform: bool = Field(default=False)

    def apply(self, df: DataFrame, engine: DictData, **kwargs) -> DataFrame:
        """Apply to Rename Column to the Polars DataFrame."""
        logger.info(f"Rename columns statement:\n{self.columns}")
        return df.rename({col.source: col.name for col in self.columns})
