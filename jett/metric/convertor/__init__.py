from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import Any, Literal, TypeVar

from ...models import MetricData


class BaseConvertor(ABC):
    """Base Convertor abstract class. Any convertor class should implement the
    convert method that will use on the tool object after execute was done.
    """

    def __init__(self, data: MetricData, custom_metric: dict[str, Any]):
        """Main initialize.

        Args:
            data (MetricData): A metric data.
            custom_metric (dict[str, Any]): A custom metric data.
        """
        self.data = data
        self.custom_metric = custom_metric

    @abstractmethod
    def convert(self) -> dict[str, Any]:
        """Covert function that making result data that from the MetricData
        model with specific use case depend on type of convert class.

        Returns:
            dict[str, Any]: A mapping metric data that was converted with the
                convertor logic.
        """


class BasicConvertor(BaseConvertor):

    def convert(self) -> dict[str, Any]:
        """Covert function with Basic metric data mode."""
        return {
            "run_result": self.data.run_result,
            "execution_time_ms": self.data.execution_time_ms,
            "engine": self.data.metric_engine.model_dump(by_alias=True),
            "source": self.data.metric_source.model_dump(by_alias=True),
            "transform": self.data.metric_transform.model_dump(by_alias=True),
            "sink": self.data.metric_sink.model_dump(by_alias=True),
            "custom_metric": self.custom_metric,
        }


class FullConvertor(BaseConvertor):

    def convert(self) -> dict[str, Any]:
        """Covert function with Full metric data mode."""
        return json.loads(self.data.model_dump_json(by_alias=True)) | {
            "custom_metric": self.custom_metric
        }


Convertor = Literal[
    "basic",
    "full",
]
ConvertType = TypeVar("ConvertType", bound=BaseConvertor)
CONVERTOR_REGISTRY: dict[Convertor, type[ConvertType]] = {
    "full": FullConvertor,
    "basic": BasicConvertor,
}
