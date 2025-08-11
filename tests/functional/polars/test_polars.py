from typing import get_type_hints

import polars as pl

from detool.engine.polars.transform.functions import RenameColumns


def test_polars():
    c = pl.col("test").cast(pl.String)
    print(type(c))


def test_polars_check_type():
    print(get_type_hints(RenameColumns.apply))
    print(get_type_hints(test_polars))
