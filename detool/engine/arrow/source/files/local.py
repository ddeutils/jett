from typing import Any, Literal

from detool import Shape
from detool.__types import DictData
from detool.models import MetricSource

from ....__abc import BaseSource


class LocalJsonFile(BaseSource):
    type: Literal["local"]
    file_format: Literal["json"]
    path: str

    def load(
        self,
        engine: DictData,
        metric: MetricSource,
        **kwargs,
    ) -> tuple[Any, Shape]: ...

    def inlet(self) -> tuple[str, str]: ...
