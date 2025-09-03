from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from pyarrow.csv import ReadOptions, read_csv
from pyarrow.json import read_json
from pydantic import Field

from jett.engine.__abc import BaseSource
from jett.models import MetricSource, Shape

if TYPE_CHECKING:
    from pyarrow import Table

    from ... import EngineContext


class LocalJsonFileTable(BaseSource):
    """Local Filesystem Json file format Source model that retrieve to the Arrow
    Table object.
    """

    type: Literal["local"] = Field(description="A local file source type.")
    arrow_type: Literal["table"] = Field(
        description="An Arrow return Table type."
    )
    file_format: Literal["json"] = Field(description="A json file format type.")
    path: str

    def load(
        self,
        engine: EngineContext,
        metric: MetricSource,
        **kwargs,
    ) -> tuple[Table, Shape]:
        """Load Json file to the Arrow Table object."""
        table: Table = read_json(self.path)
        return table, Shape.from_tuple(table.shape)

    def inlet(self) -> tuple[str, str]:
        return "local", self.path


class LocalCsvFileTable(BaseSource):
    type: Literal["local"] = Field(description="A local file source type.")
    arrow_type: Literal["table"] = Field(
        description="An Arrow return Table type."
    )
    file_format: Literal["csv"] = Field(description="A csv file format type.")
    path: str

    def load(
        self,
        engine: EngineContext,
        metric: MetricSource,
        **kwargs,
    ) -> tuple[Table, Shape]:
        table: Table = read_csv(
            self.path,
            read_options=ReadOptions(
                autogenerate_column_names=True,
            ),
        )
        return table, Shape.from_tuple(table.shape)

    def inlet(self) -> tuple[str, str]:
        return "local", self.path
