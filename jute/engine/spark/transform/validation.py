from typing import Literal

from pyspark.sql import DataFrame, SparkSession

from ....__types import DictData
from ..utils import validate_col_disallow_pattern
from .__abc import AnyApplyOutput, BaseSparkTransform


class ValidateColumnDisallowSpace(BaseSparkTransform):
    op: Literal["validate_column_names_disallow_whitespace"]

    def apply(
        self,
        df: DataFrame,
        engine: DictData,
        *,
        spark: SparkSession | None = None,
        **kwargs,
    ) -> AnyApplyOutput:
        validate_col_disallow_pattern(schema=df.schema, patterns=["whitespace"])
        return df
