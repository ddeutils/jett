from typing import Any, Literal

from __types import DictData
from models import Context, Result

from ..__abc import BaseEngine


class Polars(BaseEngine):
    type: Literal["polars"]

    def execute(
        self,
        context: Context,
        engine: DictData,
    ) -> Any: ...

    def apply(
        self,
        df: Any,
        context: Context,
        engine: DictData,
        **kwargs,
    ) -> Any: ...

    def set_result(self, df: Any, context: Context) -> Result: ...

    def set_engine_context(self, context: Context, **kwargs) -> DictData: ...
