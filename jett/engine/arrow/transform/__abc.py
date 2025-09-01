from abc import ABC, abstractmethod

from pyarrow import Schema, Table

from ....__types import DictData
from ....models import MetricOperatorOrder, MetricOperatorTransform
from ...__abc import BaseTransform


class BaseArrowTransform(BaseTransform, ABC):
    """Base Arrow Transform abstract model."""

    @abstractmethod
    def apply(
        self,
        df: Table,
        engine: DictData,
        metric: MetricOperatorOrder,
        **kwargs,
    ) -> Table:
        """Apply operator transform abstraction method."""

    @staticmethod
    def sync_schema(
        pre: Schema,
        post: Schema,
        metric: MetricOperatorTransform,
        **kwargs,
    ) -> None:
        """Sync schema change to the metric transform."""
