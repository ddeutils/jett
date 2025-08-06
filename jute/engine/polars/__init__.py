import logging
from typing import Any, Literal

from polars import DataFrame

from ...__types import DictData
from ...models import Context, Result
from ..__abc import BaseEngine
from .sink import Sink
from .source import Source

logger = logging.getLogger("jute")


class Polars(BaseEngine):
    """Polars Engine model."""

    type: Literal["polars"]
    source: Source
    sink: Sink

    def execute(
        self,
        context: Context,
        engine: DictData,
    ) -> Any:
        logger.info("Start execute with Spark engine.")
        df: DataFrame = self.source.handle_load(context, engine=engine)
        df: DataFrame = self.handle_apply(df, context, engine=engine)
        self.sink.handle_save(df, context, engine=engine)
        return df

    def apply(
        self,
        df: Any,
        context: Context,
        engine: DictData,
        **kwargs,
    ) -> Any: ...

    def set_result(self, df: Any, context: Context) -> Result: ...

    def set_engine_context(self, context: Context, **kwargs) -> DictData:
        """Set itself to engine context data."""
        return {
            "engine": self,
        }
