# Jett

[![pypi version](https://img.shields.io/pypi/v/jett)](https://pypi.org/project/jett/)
[![python support version](https://img.shields.io/pypi/pyversions/jett)](https://pypi.org/project/jett/)
[![size](https://img.shields.io/github/languages/code-size/ddeutils/jett)](https://github.com/ddeutils/jett)

**Just an Engine Template Tool** that easy to use and develop for Data Engineer.
This project support the ETL template for multiple DataFrame engine like
`PySpark`, `Duckdb`, `Polars`, etc.

**Supported Features**:

- Dynamic Supported Transform Engines via configuration
- JSON Schema Validation on any IDE.
- Support Airflow Operator

## 📦 Installation

```shell
uv pip install -U jett
```

**Engine Supported**:

| Name    | Status | Description                                           |
|---------|:------:|-------------------------------------------------------|
| Pyspark |   ✅    | Pyspark and Spark submit CLI for distributed workload |
| DuckDB  |   ✅    | DuckDB and Spark API DuckDB                           |
| Polars  |   ✅    | Polars for Python workload                            |
| Arrow   |   ✅    | Arrow for Python workflow with Columnar               |
| Daft    |   ✅    | Daft for Python distributed workload                  |
| DBT     |   ❌    | DBT for SQL workload                                  |
| GX      |   ❌    | Great Expectation for data quality                    |
