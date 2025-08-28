import pytest
from pydantic import ValidationError

from jett.models import Result, Shape


def test_models_shape():
    shape = Shape()
    assert shape.rows == 0
    assert shape.columns == 0

    shape = Shape(rows=1, columns=2)
    assert shape.rows == 1
    assert shape.columns == 2

    shape = Shape.from_tuple(data=(1, 2))
    assert shape.rows == 1
    assert shape.columns == 2

    with pytest.raises(ValidationError):
        Shape.from_tuple(data=(-1, 0))

    with pytest.raises(ValidationError):
        Shape.from_tuple(data=(0.1, 0))


def test_models_result():
    result = Result()
    assert result.data == []
    assert result.columns == []
    assert result.schema_dict == {}
