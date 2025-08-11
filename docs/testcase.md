# Testcase

## Spark

```shell
pytest -vv -s ./tests/functional/spark/test_operator.py::test_spark_csv_to_console
pytest -vv -s ./tests/functional/spark/test_operator.py::test_spark_json_to_console
```

## Duckdb

```shell
pytest -vv -s ./tests/functional/duckdb/test_operator.py::test_duckdb_csv_to_console
pytest -vv -s ./tests/functional/duckdb/test_operator.py::test_duckdb_json_to_console
```
