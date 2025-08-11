from abc import ABC

import polars as pl
from polars import DataType

from ...__abc import BaseTransform

TYPES: dict[str, type[DataType]] = {
    "string": pl.String,
    "boolean": pl.Boolean,
}


class BasePolarsTransform(BaseTransform, ABC):
    """Base Polars Transform abstract model"""
