from typing import Literal

import daft
from daft import DataFrame

from .....__types import DictData
from .....models import MetricSource, Shape
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
    ) -> tuple[DataFrame, Shape]:
        df: DataFrame = daft.read_json(
            path=self.path,
            file_path_column=None,
        )
        return df, Shape()

    def inlet(self) -> tuple[str, str]:
        return "local", self.path
