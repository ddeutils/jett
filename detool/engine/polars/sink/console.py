import logging
from typing import Any, Literal

from polars import DataFrame

from ....__types import DictData
from ....models import Shape
from ...__abc import BaseSink

logger = logging.getLogger("detool")


class Console(BaseSink):
    """Console Polars Sink model."""

    type: Literal["console"]
    limit: int = 10

    def save(
        self,
        df: DataFrame,
        *,
        engine: DictData,
        **kwargs,
    ) -> Any:
        """Save the result data to the Console."""
        logger.info("🎯 Sink - Start sync with console")
        print(df.head(n=self.limit))
        return df, Shape.from_tuple(df.shape)

    def outlet(self) -> tuple[str, str]:
        return "console", self.dest()

    def dest(self) -> str:
        return "console"
