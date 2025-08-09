# DeTool (Just a Tool for Template Engines)

Just a Tool Template Engines that easy to use for Data Engineer.
This project provide the ETL template for implemented DataFrame engine like
`PySpark`, `Duckdb`, `Polars`, etc.

## 📦 Installation

```shell
uv pip install -U detool
```

| Package |   Version    | Next Support |
|---------|:------------:|:------------:|
| Python  |  `3.10.13`   |  `>=3.11.0`  |
| Spark   |   `3.4.2`    |  `>=4.0.0`   |
| Hadoop  |     `3`      |     `3`      |
| Java    | `openjdk@11` | `openjdk@17` |
| Pyspark |   `3.4.1`    |  `>=4.0.0`   |
| Scala   |  `2.12.17`   |  `2.12.17`   |
| DuckDB  |   `1.3.2`    |              |
| Polars  |   `1.32.0`   |              |
| Arrow   |   `21.0.0`   |              |

| Name    | Status | Description                   |
|---------|:------:|-------------------------------|
| Pyspark |   ✅    | Pyspark and Spark submit CLI. |
| DuckDB  |   ✅    | DuckDB and Spark API DuckDB   |
| Arrow   |   ❌    |                               |
| Polars  |   ❌    |                               |

## 📝 Usage

For example file, `etl.spark.jude`:

```yaml
type: spark
name: Load CSV to GGSheet
app_name: load_csv_to_ggsheet
master: local
# 1) Load data from source
source:
  type: local
  file_format: csv
  path: ./assets/data/customer.csv
# 2) Transform this data.
transform:
  - op: rename_to_snakecase
  - op: group
    transform:
      - op: expr
        value: "CAST(id AS string)"
# 3) Sink result to target
sink:
  type: local
  file_type: google_sheet
  path: ./assets/landing/customer.gsheet
# 4) Metric that will send after execution.
metric:
  - type: console
    convertor: basic
  - type: restapi
    convertor: basic
    host: "localhost"
    port: 1234
```

## 📖 Document

This project will reference emoji from the [Pipeline Emojis](https://emojidb.org/pipeline-emojis).

## 💬 Contribute

I do not think this project will go around the world because it has specific propose,
and you can create by your coding without this project dependency for long term
solution. So, on this time, you can open [the GitHub issue on this project 🙌](https://github.com/ddeutils/detool/issues)
for fix bug or request new feature if you want it.
