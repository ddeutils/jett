from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from jett.__types import DictData
from jett.models import MetricOperatorOrder, MetricOperatorTransform
from jett.utils import sort_non_sensitive_str

from ...__abc import BaseTransform
from ..utils import extract_cols_without_array, schema2dict

if TYPE_CHECKING:
    from pyarrow import Schema, Table
    from pyarrow.dataset import Dataset

    TableOrDataset = Table | Dataset


class BaseArrowTransform(BaseTransform, ABC):
    """Base Arrow Transform abstract model."""

    @abstractmethod
    def apply(
        self,
        df: TableOrDataset,
        engine: DictData,
        metric: MetricOperatorOrder,
        **kwargs,
    ) -> TableOrDataset:
        """Apply operator transform abstraction method.

        Args:
            df (Table | Dataset): An Arrow Table or Dataset.
            engine (DictData): An engine context data that was created from the
                `post_execute` method. That will contain engine model, engine
                session object for this execution, or it can be specific config
                that was generated on that current execution.
            metric (MetricOperatorOrder): A metric transform that was set from
                handler step for passing custom metric data.

        Returns:
            Table | Dataset: An Arrow table that was applied.
        """

    @staticmethod
    def sync_schema(
        pre: Schema,
        post: Schema,
        metric: MetricOperatorTransform,
        **kwargs,
    ) -> None:
        """Sync schema change to the metric transform.

        Args:
            pre (Schema): A pre schema before apply.
            post (Schema): A post schema that have applied.
            metric (MetricOperatorTransform): An operator transform metric model
                that want to update schema pre and post for tracking change.
        """
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
