from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Literal

from pyarrow.csv import ParseOptions
from pyarrow.dataset import CsvFileFormat, ParquetFileFormat, dataset
from pydantic import Field

from jett.engine.__abc import BaseSource
from jett.models import MetricSource, Shape

if TYPE_CHECKING:
    from pyarrow.dataset import Dataset

    from ... import EngineContext

logger = logging.getLogger("jett")


class LocalCsvFileDataset(BaseSource):
    """Local File System with CSV file format source model."""

    type: Literal["local"] = Field(description="A local file source type.")
    arrow_type: Literal["dataset"] = Field(
        description="An Arrow return Dataset type."
    )
    file_format: Literal["csv"] = Field(description="A csv file format type.")
    path: str = Field(description="A source filesystem path.")
    partitioning: list[str] | str | None = Field(default=None)

    def load(
        self,
        engine: EngineContext,
        metric: MetricSource,
        **kwargs,
    ) -> tuple[Dataset, Shape]:
        """Load the Arrow Dataset object from the target filesystem path.

        Args:
            engine (EngineContext): An Arrow engine context data that was set
                before start execution.
            metric (MetricSource): A metric source model.

        Returns:
            tuple[Dataset, Shape]: A pair of Arrow Dataset object and its shape
                model.
        """
        ds: Dataset = dataset(
            self.path,
            partitioning="hive",
            format=CsvFileFormat(
                **{"parse_options": ParseOptions(delimiter=",")},
            ),
        )
        logger.warning(
            "If set arrow type be dataset, it will not collect the data to "
            "memory yet. So, its shape will show 0 rows."
        )
        return ds, Shape(rows=0, columns=len(ds.schema))

    def inlet(self) -> tuple[str, str]:
        return "local", self.path


class LocalParquetFileDataset(BaseSource):
    type: Literal["local"] = Field(description="A local file source type.")
    arrow_type: Literal["dataset"] = Field(
        description="An Arrow return Dataset type."
    )
    file_format: Literal["parquet"] = Field(
        description="A parquet file format type."
    )
    path: str

    def load(
        self,
        engine: EngineContext,
        metric: MetricSource,
        **kwargs,
    ) -> tuple[Dataset, Shape]:
        """Load the Arrow Dataset object from the target filesystem path.

        Args:
            engine (EngineContext): An Arrow engine context data that was set
                before start execution.
            metric (MetricSource): A metric source model.

        Returns:
            tuple[Dataset, Shape]: A pair of Arrow Dataset object and its shape
                model.
        """
        ds: Dataset = dataset(
            self.path,
            partitioning="hive",
            format=ParquetFileFormat(),
        )
        logger.warning(
            "If set arrow type be dataset, it will not collect the data to "
            "memory yet. So, its shape will show 0 rows."
        )
        return ds, Shape(rows=0, columns=len(ds.schema))

    def inlet(self) -> tuple[str, str]:
        return "local", self.path
