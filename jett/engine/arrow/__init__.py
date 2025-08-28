import logging
from typing import Literal

from pyarrow import Table
from pyarrow.dataset import Dataset
from pydantic import Field

from ... import Result
from ...__types import DictData
from ...models import ColDetail, Context, MetricEngine, MetricTransform
from ..__abc import BaseEngine
from .sink import Sink
from .source import Source

logger = logging.getLogger("jett")


class Arrow(BaseEngine):
    """Arrow Engine Model."""

    type: Literal["arrow"] = Field(description="An engine type.")
    sink: list[Sink]
    source: Source

    def execute(
        self,
        context: Context,
        engine: DictData,
        metric: MetricEngine,
    ) -> Table | Dataset:
        logger.info("Start execute with Arrow engine.")
        df: Table = self.source.handle_load(context, engine=engine)
        df: Table = self.handle_apply(df, context, engine=engine)
        for sk in self.sink:
            sk.handle_save(df, context, engine=engine)
        return df

    def set_engine_context(self, context: Context, **kwargs) -> DictData:
        return {
            "engine": self,
        }

    def set_result(self, df: Table, context: Context) -> Result:
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
        engine: DictData,
        metric: MetricTransform,
        **kwargs,
    ) -> Table:
        return df
