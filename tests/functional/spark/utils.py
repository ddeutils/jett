from pyspark.sql.types import DataType, StructField


def field(name: str, t: DataType, nullable: bool = True) -> StructField:
    return StructField(name, t, nullable)
