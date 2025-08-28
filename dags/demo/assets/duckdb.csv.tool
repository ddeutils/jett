type: duckdb
name: load_customer_to_raw
author: data-engineering
tags: ["duckdb", "example"]

source:
  type: local
  file_format: csv
  path: "./assets/data/customers-1000.csv"
  delimiter: ","
  header: true
  sample_records: 20

transforms:
  - op: rename_snakecase

  - op: sql
    sql_file: import_statement.sql

  - op: sql
    priority: pre
    sql: |
      SELECT
        index
        , customer_id
        , email
        , subscription_date
      FROM df

  - op: rename_columns
    priority: post
    columns:
      - name: id
        source: index

sink:
  type: console

metrics:
  - type: console
