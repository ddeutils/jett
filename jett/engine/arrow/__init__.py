from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Literal, TypedDict

from pydantic import Field
from pydantic.functional_validators import field_validator

from ... import Result
from ...__types import DictData
from ...models import ColDetail, Context, MetricEngine, MetricTransform
from ..__abc import BaseEngine
from .sink import Sink
from .source import Source
from .transform import Transform

if TYPE_CHECKING:
    from pyarrow import Table

logger = logging.getLogger("jett")


class EngineContext(TypedDict):
    """Engine Context dict typed for Arrow engine execution."""

    engine: Arrow


class Arrow(BaseEngine):
    """Arrow Engine Model.

    Notes:
        This engine support multiple sink.
    """

    type: Literal["arrow"] = Field(description="An Arrow engine type.")
    source: Source = Field(description="A Source model.")
    transforms: list[Transform] = Field(
        default_factory=list, description="A list of Transform models."
    )
    sink: list[Sink] = Field(
        description="A list of Sink model. It allow to pass with a Sink model.",
        default_factory=list,
    )
    enable_collect_result: bool = Field(default=False)

    @field_validator(
        "sink",
        mode="before",
        json_schema_input_type=Sink | list[Sink],
    )
    def __prepare_sink(cls, data: Any) -> Any:
        """Prepare the sink field value that should be list of Sink model."""
        return [data] if not isinstance(data, list) else data

    def execute(
        self, context: Context, engine: DictData, metric: MetricEngine
    ) -> Table:
        """Execute Arrow engine method.

        Args:
            context (Context): A execution context that was created from the
                core operator execution step this context will keep all operator
                metadata and metric data before emit them to metric config
                model.
            engine (DictData): An engine context data that was created from the
                `post_execute` method. That will contain engine model, engine
                session object for this execution, or it can be specific config
                that was generated on that current execution.
            metric (MetricEngine): A metric engine that was set from handler
                step for passing custom metric data.

        Returns:
            Table: A result Arrow Table API.
        """
        logger.info("🏗️ Start execute with Arrow engine.")

        # NOTE: Start run source handler.
        df: Table = self.source.handle_load(context, engine=engine)

        # NOTE: Start run transform handler.
        df: Table = self.handle_apply(df, context, engine=engine)

        # NOTE: Start run sink handler with sequential strategy.
        for sk in self.sink:
            sk.handle_save(df, context, engine=engine)
        return df

    def set_engine_context(self, context: Context, **kwargs) -> EngineContext:
        """Create Arrow Engine Context data for passing to the execute method.

        Args:
            context (Context): A execution context that was created from the
                core operator execution step this context will keep all operator
                metadata and metric data before emit them to metric config
                model.

        Returns:
            EngineContext: A mapping of necessary data for Arrow execution
                context.
        """
        return {
            "engine": self,
        }

    def set_result(self, df: Table, context: Context) -> Result:
        """Set the Result object for this Spark engine.

        Args:
            df (Table): An Arrow Table.
            context (Context): A execution context that was created from the
                core operator execution step this context will keep all operator
                metadata and metric data before emit them to metric config
                model.

        Returns:
            Result: A result object that catch schema and sample data from the
                execution result Table.
        """
        if self.enable_collect_result:
            logger.warning(
                "⚠️ If you collect results from the Table, it will fetch all "
                "records in the Table. This is not ideal for any ETL "
                "pipeline with large data sizes. Please use it only for "
                "testing purposes or with smaller datasets."
            )
        return Result(
            data=[],
            columns=[
                ColDetail(name=field.name, dtype=str(field.type))
                for field in df.schema
            ],
            schema_dict={field.name: field.type for field in df.schema},
        )

    def apply(
        self,
        df: Table,
        context: Context,
        engine: EngineContext,
        metric: MetricTransform,
        **kwargs,
    ) -> Table:
        """Apply Arrow engine transformation to the source. This method will
        apply all operators by priority.

        Args:
            df (Table): An Arrow Table.
            context (Context): A execution context that was created from the
                core operator execution step this context will keep all operator
                metadata and metric data before emit them to metric config
                model.
            engine (EngineContext): An engine context data that was created from
                the `post_execute` method. That will contain engine model, engine
                session object for this execution, or it can be specific config
                that was generated on that current execution.
            metric (MetricTransform): A metric transform that was set from
                handler step for passing custom metric data.

        Returns:
            Table:
        """
        logger.debug(f"⚙️ Priority - transform count: {len(self.transforms)}")
        for op in self.transforms:
            df: Table = op.handle_apply(df, context, engine=engine)
        return df
