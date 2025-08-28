from collections.abc import Sequence
from typing import Any

from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context


class JettOperator(BaseOperator):
    """Jett Airflow Operator object."""

    template_fields: Sequence[str] = ("tool",)
    template_ext: Sequence[str] = (".yml", ".yaml")

    def __init__(self, tool: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.tool: str = tool

    def execute(self, context: Context) -> Any: ...
