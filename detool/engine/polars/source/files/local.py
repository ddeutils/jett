from pathlib import Path
from typing import Literal

import polars as pl
from polars import DataFrame
from pydantic import Field

from .....__types import DictData
from .....models import Shape
from ....__abc import BaseSource


class LocalCSVFile(BaseSource):
    """Local CSV file data source."""

    type: Literal["local"]
    file_format: Literal["csv"]
    path: str
    delimiter: str = "|"
    header: bool = Field(default=True)
    sample_records: int | None = 200

    def load(self, engine: DictData, **kwargs) -> tuple[DataFrame, Shape]:
        """Load CSV file to DuckDB Relation object."""
        file_format: str = Path(self.path).suffix
        if file_format not in (".csv",):
            raise NotImplementedError(
                f"Local file format: {file_format!r} does not support."
            )
        df = pl.read_csv(
            source=self.path,
            delimiter=self.delimiter,
            has_header=self.header,
            sample_size=self.sample_records,
        )
        return df, Shape.from_tuple(df.shape)

    def inlet(self) -> tuple[str, str]:
        return "local", self.path


class LocalJsonFile(BaseSource):
    """Local JSON file data source."""

    type: Literal["local"]
    file_format: Literal["json"]
    path: str

    def load(self, engine: DictData, **kwargs) -> tuple[DataFrame, Shape]:
        file_format: str = Path(self.path).suffix
        if file_format not in (".json",):
            raise NotImplementedError(
                f"Local file format: {file_format!r} does not support."
            )
        df: DataFrame = pl.read_json(source=self.path)
        return df, Shape.from_tuple(df.shape)

    def inlet(self) -> tuple[str, str]:
        return "local", self.path
