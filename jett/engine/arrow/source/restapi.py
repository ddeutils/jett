from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Literal

from pydantic import Field

from jett import Shape
from jett.engine.__abc import BaseSource
from jett.models import MetricSource

if TYPE_CHECKING:
    from pyarrow.dataset import Dataset

    from .. import EngineContext

logger = logging.getLogger("jett")


class RestApi(BaseSource):
    """RestAPI source model."""

    type: Literal["restapi"] = Field(description="A RestAPI source type.")
    base_url: str = Field(description="A base URL.")
    path: str = Field(description="A URL path that will combine with the base.")
    method: Literal["GET", "POST"] = Field(
        description="A method type of RestAPI."
    )
    query: dict[str, Any] = Field(default_factory=dict)

    def load(
        self,
        engine: EngineContext,
        metric: MetricSource,
        **kwargs,
    ) -> tuple[Dataset, Shape]:
        """Load data from Rest API"""
        logger.info("Start loading data from the source URL.")
        return None, Shape()

    def inlet(self) -> tuple[str, str]:
        return "api", self.base_url
