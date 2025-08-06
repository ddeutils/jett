# JUTE (Just a Utility Template Engines)

Just a Utility Template Engines. This project provide the ETL template for
implemented DataFrame engine like `PySpark`, `Duckdb`, `Polars`, etc.

## 📦 Installation

```shell
uv pip install -U jute
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

| Name    | Status | Description                             |
|---------|:------:|-----------------------------------------|
| Pyspark |   ✅    | ETL with Pyspark and Spark submit CLI.  |
| DuckDB  |   ✅    |                                         |
| Arrow   |   ❌    |                                         |
| Polars  |   ❌    |                                         |

## Usage

For example file, `etl.spark.jude`:

```yaml
type: spark
name: Load CSV to GGSheet
app_name: load_csv_to_ggsheet
source:
  type: local
  file_format: csv
  path: ./assets/data/customer.csv
transform:
  - op: rename_to_snakecase
  - op: group
    transform:
      - op: expr
        value: "CAST(id AS string)"
sink:
  type: local
  file_type: google_sheet
  path: ./assets/landing/customer.gsheet
metric:
  - type: console
    convertor: basic
  - type: restapi
    convertor: basic
    host: "localhost"
    port: 1234
```

## Document

This project will reference emoji from the [Pipeline Emojis](https://emojidb.org/pipeline-emojis).

## 💬 Contribute

I do not think this project will go around the world because it has specific propose,
and you can create by your coding without this project dependency for long term
solution. So, on this time, you can open [the GitHub issue on this project 🙌](https://github.com/ddeutils/jude/issues)
for fix bug or request new feature if you want it.
