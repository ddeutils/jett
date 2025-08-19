import duckdb
import pytest

from jett.engine.duckdb.transform.functions import SQLExecute


@pytest.fixture(scope="module")
def mock_relation() -> duckdb.DuckDBPyRelation:
    return duckdb.sql(
        """
        SELECT
            gen_random_uuid()   AS id
            , concat(
                    'value is '
                    , case when mod(range,2) = 0 then 'even' else 'uneven' end
                )               AS description
            , range             AS value
            , now() + concat(range,' ', 'minutes')::interval AS created_timestamp
        FROM range(1, 10)
        """
    )


def test_transform_sql(mock_relation):
    transform = SQLExecute.model_validate(
        {"op": "sql", "sql": "SELECT * FROM df;"}
    )
    assert (
        transform.apply(mock_relation, engine={}).shape == mock_relation.shape
    )
