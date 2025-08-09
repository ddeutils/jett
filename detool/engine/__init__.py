from typing import Annotated, Union

from pydantic import Field

from .arrow import Arrow
from .duckdb import DuckDB
from .polars import Polars
from .spark import Spark

Registry = Annotated[
    Union[
        Arrow,
        Polars,
        DuckDB,
        Spark,
    ],
    Field(
        discriminator="type",
        description="The main engine registry models for operator model.",
    ),
]
