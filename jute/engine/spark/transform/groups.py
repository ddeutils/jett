from typing import Annotated, Literal, Union

from pydantic import Field
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import expr

from ....__types import DictData
from ....errors import JuteTransformError
from .__abc import AnyApplyOutput, BaseSparkTransform, PairCol, is_pair_col


class Expr(BaseSparkTransform):
    """Expression Transform model."""

    op: Literal["expr"]
    name: str = Field(
        description=(
            "An alias name of this output of the query expression result store."
        )
    )
    query: str = Field(description="An expression query.")

    def apply(
        self,
        df: DataFrame,
        engine: DictData,
        *,
        spark: SparkSession | None = None,
        **kwargs,
    ) -> PairCol:
        """Apply priority transform to expression the query.

        Args:
            df (DataFrame): A Spark DataFrame instance.
            spark (SparkSession): A Spark Session.
            engine (DictData): An engine context data.
        """
        return expr(self.query), self.name


GroupOp = Annotated[Union[Expr,], Field(discriminator="op")]


class Group(BaseSparkTransform):
    """Group Operator transform model."""

    op: Literal["group"]
    transform: list[GroupOp]

    def apply(
        self,
        df: DataFrame,
        engine: DictData,
        *,
        spark: SparkSession | None = None,
        **kwargs,
    ) -> AnyApplyOutput:
        maps: dict[str, Column] = {}
        for g in self.transform:
            output: PairCol | list[PairCol] = g.apply(
                df, engine=engine, spark=spark
            )
            if is_pair_col(output):
                rs: dict[str, Column] = {output[1]: output[2]}
            elif isinstance(output, list) and all(
                is_pair_col(i) for i in output
            ):
                rs: dict[str, Column] = {p[1]: p[0] for p in output}
            else:
                raise JuteTransformError(
                    "Transform group on Spark engine should return the apply group "
                    "result with a pair of Column and str only"
                )
            maps.update(rs)
        df: DataFrame = df.withColumns(maps)
        return df
