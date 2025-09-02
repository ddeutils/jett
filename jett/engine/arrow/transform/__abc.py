from abc import ABC, abstractmethod

from pyarrow import Schema, Table

from jett.__types import DictData
from jett.models import MetricOperatorOrder, MetricOperatorTransform
from jett.utils import sort_non_sensitive_str

from ...__abc import BaseTransform
from ..utils import extract_cols_without_array, schema2dict


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
        pre_no_array = sort_non_sensitive_str(
            extract_cols_without_array(schema=pre)
        )
        post_no_array = sort_non_sensitive_str(
            extract_cols_without_array(schema=post)
        )
        # NOTE: Start update the pre- and post-schema metric.
        metric.transform_pre = {
            "schema": schema2dict(pre, sorted_by_name=True),
            "schema_no_array": pre_no_array,
        }
        metric.transform_post = {
            "schema": schema2dict(post, sorted_by_name=True),
            "schema_no_array": post_no_array,
        }
