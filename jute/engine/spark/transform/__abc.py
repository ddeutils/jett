import logging
from abc import ABC, abstractmethod
from typing import Any, Literal

from pydantic import Field
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.connect.column import Column as ColumnRemote
from pyspark.sql.connect.session import DataFrame as DataFrameRemote

from ....__types import DictData
from ....errors import JuteTransformError
from ....models import MetricOperator, MetricTransformOrder
from ...__abc import BaseTransform
from ..__types import AnyDataFrame, PairCol

AnyApplyOutput = PairCol | list[PairCol] | AnyDataFrame

logger = logging.getLogger("jute")


def is_pair_col(value: PairCol | Any) -> bool:
    """Change value is a pair of Column and string.

    Args:
        value (PairCol | Any): A pair of Column and its name or any value.

    Returns:
        bool: Return True if an input value is a pair of Column and alias.
    """
    return (
        isinstance(value, tuple)
        and len(value) == 2
        and isinstance(value[0], (Column, ColumnRemote))
        and isinstance(value[1], str)
    )


class BaseSparkTransform(BaseTransform, ABC):
    """Base Spark Transform abstract model."""

    priority: Literal["pre", "group", "post"] = Field(
        default="pre",
        description=(
            "A priority value for order transform operator before apply."
        ),
    )
    cache: bool = Field(
        default=False,
        description="Use `.cache` method to applied Spark DataFrame if it set.",
    )

    @abstractmethod
    def apply(
        self,
        df: DataFrame,
        engine: DictData,
        *,
        spark: SparkSession | None = None,
        **kwargs,
    ) -> AnyApplyOutput:
        """Apply operator transform abstraction method.

        Args:
            df (Any): A Spark DataFrame.
            engine (DictData): An engine context data that was created from the
                `post_execute` method. That will contain engine model, engine
                session object for this execution, or it can be specific config
                that was generated on that current execution.
            spark (SparkSession, default None): A Spark session.
        """

    def apply_group(
        self,
        df: DataFrame,
        engine: DictData,
        *,
        spark: SparkSession | None = None,
        **kwargs,
    ) -> PairCol | list[PairCol]:
        """Apply group transform."""
        raise NotImplementedError(
            f"Transform: {self.__class__.__name__} on Spark engine does not "
            f"implement the group operation, please change this to priority "
            f"operator."
        )

    def handle_apply_group(
        self,
        df: DataFrame,
        context: DictData,
        engine: DictData,
        *,
        spark: SparkSession | None = None,
        **kwargs,
    ) -> dict[str, Column]:
        """Handle Apply group transform."""
        metric = MetricOperator(type="order", transform_op=self.op)
        context["metric_group_transform"].transforms.append(metric)
        logger.info(f"🔨 Handle Apply Group Operator: {self.op!r}")
        try:
            output: PairCol | list[PairCol] = self.apply_group(
                df, engine, spark=spark, **kwargs
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
            if self.cache:
                logger.warning(
                    f"🏭 Cache the DataFrame after apply operator: {self.op!r}"
                )
                df.cache()
            return rs
        finally:
            metric.finish()

    def handle_apply(
        self,
        df: AnyDataFrame,
        context: DictData,
        engine: DictData,
        spark: SparkSession | None = None,
        **kwargs,
    ) -> AnyDataFrame:
        """Handle the Operator Apply result output from its custom apply that
        can make different type of result. It can be Column, DataFrame, or
        a pair of Column and its alias name.

        Args:
            df (AnyDataFrame): A Spark DataFrame.
            context: (Context): A execution context that was created from the
                core operator execution step this context will keep all operator
                metadata and metric data before emit them to metric config
                model.
            engine (DictData): An engine context data that was created from the
                `post_execute` method. That will contain engine model, engine
                session object for this execution, or it can be specific config
                that was generated on that current execution.
            spark (SparkSession, default None): A Spark session.

        Returns:
            AnyDataFrame: A applied Spark DataFrame.
        """
        metric = MetricTransformOrder(type="order", transform_op=self.op)
        context["metric_operator"].append(metric)
        logger.info(f"👷🔧 Handle Apply Operator: {self.op!r}")
        try:
            output: AnyApplyOutput = self.apply(df, engine=engine, **kwargs)
            if is_pair_col(output):
                df = df.withColumn(output[1], output[0])
            elif isinstance(output, list) and all(
                is_pair_col(i) for i in output
            ):
                df = df.withColumns({i[1]: i[0] for i in output})
            elif isinstance(output, (DataFrame, DataFrameRemote)):
                df: AnyDataFrame = output
            else:
                metric.add(key=self.op, value=output)
                logger.info(f"Set metric from func {self.op} completely")

                # ARCHIVE:
                # raise JuteTransformError(
                #     "Transform priority or fallback on Spark engine should return "
                #     "the apply result with type in a pair of Column, a list of "
                #     "pair of Column, or DataFrame only"
                # )
            if self.cache:
                logger.warning(
                    f"🏭 Cache the DataFrame after apply operator: {self.op!r}"
                )
                df.cache()
            return df
        finally:
            metric.finish()
