from typing import Annotated, Union

from pydantic import Field

from .duckdb import DuckDB
from .spark import Spark

Registry = Annotated[
    Union[
        # Pandas2,
        # Arrow,
        # Polars,
        DuckDB,
        Spark,
    ],
    Field(
        discriminator="type",
        description="The main engine registry models for operator model.",
    ),
]
