from typing import Any, Literal

from daft import DataFrame

from ... import Result
from ...__types import DictData
from ...models import Context, MetricEngine, MetricTransform
from ..__abc import BaseEngine


class Daft(BaseEngine):
    type: Literal["daft"]

    def execute(
        self,
        context: Context,
        engine: DictData,
        metric: MetricEngine,
    ) -> Any: ...

    def set_engine_context(self, context: Context, **kwargs) -> DictData: ...

    def set_result(self, df: DataFrame, context: Context) -> Result: ...

    def apply(
        self,
        df: DataFrame,
        context: Context,
        engine: DictData,
        metric: MetricTransform,
        **kwargs,
    ) -> Any: ...
