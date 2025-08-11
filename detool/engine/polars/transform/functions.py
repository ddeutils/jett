import logging
from pathlib import Path
from typing import Literal

import polars as pl
from polars import DataFrame
from pydantic import Field
from pydantic.functional_validators import model_validator
from typing_extensions import Self

from ....__types import DictData
from ....errors import ToolTransformError
from ....models import MetricOperatorOrder
from ....utils import to_snake_case
from ...__abc import BaseEngine
from .__abc import AnyApplyOutput, BasePolarsTransform
from .__models import ColMap
from .__types import PairExpr

logger = logging.getLogger("detool")


class RenameSnakeCase(BasePolarsTransform):
    op: Literal["rename_snakecase"]

    def apply(
        self,
        df: DataFrame,
        engine: DictData,
        metric: MetricOperatorOrder,
        **kwargs,
    ) -> DataFrame:
        rename_mapping = {col: to_snake_case(col) for col in df.columns}
        return df.rename(rename_mapping)


class RenameColumns(BasePolarsTransform):
    """Rename Columns Transform model."""

    op: Literal["rename"]
    columns: list[ColMap] = Field(description="A list of ColMap object.")
    allow_group_transform: bool = Field(default=False)

    def apply(self, df: DataFrame, engine: DictData, **kwargs) -> DataFrame:
        """Apply to Rename Column to the Polars DataFrame.

        Args:
            df (DataFrame): A Polars DataFrame.
            engine (DictData): An engine context data that was created from the
                `post_execute` method. That will contain engine model, engine
                session object for this execution, or it can be specific config
                that was generated on that current execution.
        """
        logger.info(f"Rename columns statement:\n{self.columns}")
        return df.rename({col.source: col.name for col in self.columns})


class Expr(BasePolarsTransform):
    op: Literal["expr"]
    name: str
    query: str

    def apply(
        self,
        df: DataFrame,
        engine: DictData,
        metric: MetricOperatorOrder,
        **kwargs,
    ) -> PairExpr:
        """Apply to Expr.

        Args:
            df (DataFrame): A Polars DataFrame.
            engine (DictData): An engine context data that was created from the
                `post_execute` method. That will contain engine model, engine
                session object for this execution, or it can be specific config
                that was generated on that current execution.
            metric (MetricOperatorOrder): A metric transform that was set from
                handler step for passing custom metric data.

        Returns:
            PairExpr: A pair of Expr object and its alias name.
        """
        return pl.sql_expr(self.query), self.name


class Sql(BasePolarsTransform):
    op: Literal["sql"]
    sql: str | None = Field(default=None)
    sql_file: str | None = Field(default=None)

    @model_validator(mode="after")
    def __check_sql(self) -> Self:
        """Check the necessary field of SQL use pass only SQL statement nor
        SQL filepath.
        """
        if not self.sql and not self.sql_file:
            raise ValueError("SQL and SQL file should not be empty together.")
        elif self.sql and self.sql_file:
            logger.warning(
                "If pass SQL statement and SQL file location together, it will "
                "use SQL statement first."
            )
        return self

    def apply(
        self,
        df: DataFrame,
        engine: DictData,
        metric: MetricOperatorOrder,
        **kwargs,
    ) -> DataFrame:
        logger.info("Start SQL transform ...")
        if self.sql:
            sql: str = self.sql
        else:
            _engine: BaseEngine = engine["engine"]
            sql_file: Path = _engine.parent_dir / self.sql_file
            try:
                sql: str = sql_file.read_text()
            except FileNotFoundError:
                raise ToolTransformError(
                    f"SQL file does not exists {sql_file.resolve()}"
                ) from None
        return df.sql(sql)


class SelectColumns(BasePolarsTransform):

    op: Literal["select"]
    columns: list[str]

    def apply(
        self,
        df: DataFrame,
        engine: DictData,
        metric: MetricOperatorOrder,
        **kwargs,
    ) -> AnyApplyOutput: ...
