# Arrow

## Abstract Arrow Type

Now, Arrow engine support returning types, `Table` and `Dataset`. The different
between these types:

| Feature           | `pyarrow.Table`                                | `pyarrow.dataset.Dataset`                                               |
|-------------------|------------------------------------------------|-------------------------------------------------------------------------|
| Data Scope        | Single, in-memory dataset                      | Potentially multi-file, larger-than-memory dataset                      |
| Loading           | Entirely loaded into memory                    | Lazy loading, optimized for large datasets                              |
| Compute Functions | Rich set of direct data manipulation functions | Focus on logical plan and optimized I/O, less direct compute functions  |
| Use Case          | Data wrangling, in-memory analysis             | Handling large, distributed, and partitioned datasets                   |
