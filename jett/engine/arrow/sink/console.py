import logging
from typing import TYPE_CHECKING, Any, Literal

from pydantic import Field

from jett.__types import DictData
from jett.models import Shape

from ...__abc import BaseSink

if TYPE_CHECKING:
    from pyarrow import Table

logger = logging.getLogger("jett")


class Console(BaseSink):
    """Console DuckDB Sink model."""

    type: Literal["console"] = Field(description="A console sink type.")
    limit: int = Field(
        default=10,
        description="A limit record number that want to show on the console.",
    )

    def save(
        self,
        df: Table,
        *,
        engine: DictData,
        **kwargs,
    ) -> Any:
        """Save the result data to the Console."""
        logger.info("🎯 Sink - Start sync with console")
        print(df.slice(0, self.limit))
        return df, Shape.from_tuple(df.shape)

    def outlet(self) -> tuple[str, str]:
        return "console", self.dest()

    def dest(self) -> str:
        return "console"
