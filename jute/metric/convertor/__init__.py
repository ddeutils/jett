import json
from collections.abc import Callable
from typing import Any, Literal

from ...models import MetricData


def full_convert(
    data: MetricData,
    custom_metric: dict[str, Any],
) -> dict[str, Any]:
    """Covert function with Full metric data mode.

    Args:
        data (MetricData): A metric data.
        custom_metric (dict[str, Any]): A custom metric data.
    """
    return json.loads(data.model_dump_json(by_alias=True)) | {
        "custom_metric": custom_metric
    }


def basic_convert(
    data: MetricData,
    custom_metric: dict[str, Any],
) -> dict[str, Any]:
    """Covert function with Basic metric data mode.

    Args:
        data (MetricData): A metric data.
        custom_metric (dict[str, Any]): A custom metric data.
    """
    return {
        "run_result": data.run_result,
        "execution_time_ms": data.execution_time_ms,
        "engine": data.metric_engine.model_dump(by_alias=True),
        "source": data.metric_source.model_dump(by_alias=True),
        "transform": data.metric_transform.model_dump(by_alias=True),
        "sink": data.metric_sink.model_dump(by_alias=True),
        "custom_metric": custom_metric,
    }


Convertor = Literal[
    "basic",
    "full",
]
ConvertFunc = Callable[[MetricData, dict[str, Any]], dict[str, Any]]
CONVERTOR_REGISTRY: dict[Convertor, ConvertFunc] = {
    "full": full_convert,
    "basic": basic_convert,
}
