# Testcase

Note for run unittest on the local.
## Functions

### Spark

```shell
pytest -vv -s ./tests/functional/spark/test_tool.py::test_spark_csv_to_console
pytest -vv -s ./tests/functional/spark/test_tool.py::test_spark_json_to_console
```

### Duckdb

```shell
pytest -vv -s ./tests/functional/duckdb/test_tool.py::test_duckdb_csv_to_console
pytest -vv -s ./tests/functional/duckdb/test_tool.py::test_duckdb_json_to_console
```

### Polars

```shell
pytest -vv -s ./tests/functional/polars/test_tool.py::test_polars_csv_to_console
pytest -vv -s ./tests/functional/polars/test_tool.py::test_polars_json_to_console
```

### Arrow

```shell
pytest -vv -s ./tests/functional/arrow/test_tool.py::test_arrow_csv_to_console
pytest -vv -s ./tests/functional/arrow/test_tool.py::test_arrow_json_to_console
```

### Daft

```shell
pytest -vv -s ./tests/functional/daft/test_tool.py::test_daft_csv_to_console
```

## Airflow

1. Start provision Airflow Standalone:

    ```shell
    docker compose -f ./.container/docker-compose-local-standalone.yml --env-file .env up -d
    ```

2. User & Password will show on the Docker Container console.

3. After finish, Down Airflow Standalone:

    ```shell
    docker compose -f ./.container/docker-compose-local-standalone.yml --env-file .env down --rmi all
    ```
