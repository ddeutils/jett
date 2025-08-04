import logging
from datetime import date
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from ..sink import Sink

from pydantic import BaseModel, Field
from pydantic.functional_validators import field_validator, model_validator
from pyspark.sql import Column, DataFrame, Row, SparkSession
from pyspark.sql.functions import (
    col,
    explode,
    explode_outer,
    expr,
    lit,
    posexplode,
    posexplode_outer,
    when,
)
from pyspark.sql.functions import max as spark_max
from pyspark.sql.types import StringType
from typing_extensions import Self

from ....__types import DictData
from ....utils import get_dt_now, get_random_str_unique, to_snake_case
from ..utils import (
    extract_col_with_pattern,
    extract_columns_without_array,
    extract_selectable_columns,
    has_fix_array_index,
    is_remote_session,
    is_table_exist,
    replace_all_occurrences,
    replace_fix_array_index_with_x_index,
)
from .__abc import AnyApplyOutput, BaseSparkTransform, PairCol

logger = logging.getLogger("jute")


class ColumnMap(BaseModel):
    """Column Map model."""

    name: str = Field(description="A new column name.")
    source: str = Field(
        description="A source column statement before alias with alias.",
    )
    dtype: str | None = Field(default=None, description="A data type")
    allow_quote: bool = True

    def get_null_pair(self) -> PairCol:
        """Create a new null column with specific data type.

        Returns:
            PairCol: A pair of Column instance and its alias name.
        """
        column: Column = lit(None)
        if self.dtype:
            column = column.cast(self.dtype)
        return column, self.name


class Expression(BaseSparkTransform):
    """Expression Transform model."""

    op: Literal["expr"]
    name: str = Field(
        description=(
            "An alias name of this output of the query expression result store."
        )
    )
    query: str = Field(description="An expression query.")

    def apply(
        self,
        df: DataFrame,
        engine: DictData,
        *,
        spark: SparkSession | None = None,
        **kwargs,
    ) -> PairCol:
        """Apply priority transform to expression the query.

        Args:
            df (DataFrame): A Spark DataFrame instance.
            spark (SparkSession): A Spark session.
            engine (DictData): An engine context data.
        """
        return expr(self.query), self.name


class SQL(BaseSparkTransform):
    """SQL Transform model."""

    op: Literal["sql_transform"]
    query: str = Field(description="A query statement.")

    def apply(
        self,
        df: DataFrame,
        engine: DictData,
        *,
        spark: SparkSession | None = None,
        **kwargs,
    ) -> DataFrame:
        temp_table_name: str = f"temp_table_{get_random_str_unique()}"
        query = replace_all_occurrences(
            self.query, "temp_table", temp_table_name
        )
        df.createOrReplaceTempView(temp_table_name)
        return spark.sql(query)


class RenameSnakeCase(BaseSparkTransform):
    op: Literal["rename_snakecase"]
    allow_group_transform: bool = False

    def apply(
        self,
        df: DataFrame,
        engine: DictData,
        *,
        spark: SparkSession | None = None,
        **kwargs,
    ) -> DataFrame:
        """Apply to Rename all columns (support nested columns) to snake case format.
        by default the schema and data when reading from source is already ordered.
        [caution] this transform can result in wrong data when the order of
            nested column is not an alphabetical order. the order of 1st lv column can be unordered.
        [hint] the columns can be ordered by using the logic df.select(sorted(df.columns)).
        ------
        Examples:
            Case of unordered nested columns

            input_schema = StructType([
                StructField("parent_struct", StructType([
                    StructField("FIRST_KEY", BooleanType(), True),
                    StructField("SECOND_KEY", BooleanType(), True),
                    ]),
                    True
                )
            ])
            input_data = [{
                "parent_struct": {
                    "SECOND_KEY": False,
                    "FIRST_KEY": True
                }
            }]

            Output after renaming columns will be:

            new_schema = StructType([
                StructField("parent_struct", StructType([
                    StructField("first_key", BooleanType(), True),
                    StructField("second_key", BooleanType(), True),
                    ]),
                    True
                )
            ])
            new_data = [{
                "parent_struct": {
                    "first_key": False,
                    "second_key": True
                }
            }]
        """
        non_snake_case_cols = extract_col_with_pattern(
            schema=df.schema, patterns=["non_snake_case"]
        )
        if len(non_snake_case_cols) == 0:
            return df

        final_cols = []
        transform_dict = {}
        df = df.select(sorted(df.columns))  # always sort columns
        for schema in df.schema:
            new_col_name = None
            temp_dict = {}

            if schema.name.islower() is False or " " in schema.name:
                new_col_name = to_snake_case(schema.name)
                temp_dict[new_col_name] = col(schema.name)

            dtype = schema.dataType.simpleString()
            if dtype.islower() is False or " " in dtype:
                new_dtype = to_snake_case(dtype)
                if len(temp_dict) > 0:
                    val = temp_dict[new_col_name]
                    temp_dict[new_col_name] = val.cast(new_dtype)
                else:
                    temp_dict[schema.name] = col(schema.name).cast(new_dtype)

            if len(temp_dict) > 0:
                transform_dict = {**transform_dict, **temp_dict}
            else:
                final_cols.append(schema.name)

        if len(transform_dict) > 0:
            non_snake_case_cols = extract_col_with_pattern(
                schema=df.schema, patterns=["non_snake_case"]
            )
            logger.info(
                "found non snake case columns: %s",
                "\n".join(non_snake_case_cols),
            )
            logger.info("rename to snake case columns")
            df = df.withColumns(transform_dict)
            final_cols = final_cols + list(transform_dict.keys())
            df = df.select(*final_cols)
        return df


class RenameCol(ColumnMap):

    def get_rename_pair(self) -> PairCol:
        column: Column = (
            expr(self.source)
            if has_fix_array_index(self.source)
            else col(self.source)
        )
        if self.dtype:
            column = column.cast(self.dtype)

        return column, self.name

    def get_rename_pair_fix_non_existed_by_null(self, df: DataFrame) -> PairCol:
        _rename_cache_select_cols: list[str] | None = None
        if not _rename_cache_select_cols:
            _rename_cache_select_cols = extract_selectable_columns(df.schema)

        rep_from_col: str = self.source
        if has_fix_array_index(rep_from_col):
            rep_from_col = replace_fix_array_index_with_x_index(rep_from_col)

        if rep_from_col not in _rename_cache_select_cols:
            logger.info(
                f"Fill null on column: {self.name} (dtype {self.dtype}) due to "
                f"column {self.source} not found",
            )
            return self.get_null_pair()
        return self.get_rename_pair()


class RenameColumns(BaseSparkTransform):
    """Rename Columns Transform model."""

    op: Literal["rename_columns"]
    columns: list[RenameCol] = Field(description="A list of RenameCol model.")
    allow_fill_null_when_col_not_exist: bool = False

    @model_validator(mode="after")
    def __allow_fill_null_rule(self) -> Self:
        """Validate each column model with allow autofill null on not-existing
        column need its data type.
        """
        if self.allow_fill_null_when_col_not_exist:
            if any(c is None for c in self.columns):
                raise ValueError(
                    "`allow_fill_null_when_col_not_exist` property must use "
                    "with `dtype` property"
                )
        return self

    def apply(
        self,
        df: DataFrame,
        engine: DictData,
        *,
        spark: SparkSession | None = None,
        **kwargs,
    ) -> list[PairCol]:
        """Apply to Rename Column transform."""
        map_cols: list[PairCol] = []
        for c in self.columns:
            if self.allow_fill_null_when_col_not_exist:
                map_cols.append(c.get_rename_pair_fix_non_existed_by_null(df))
            else:
                map_cols.append(c.get_rename_pair())
        logger.info(f"Rename columns statement:\n{map_cols}")
        return map_cols

    def apply_group(
        self,
        df: DataFrame,
        engine: DictData,
        *,
        spark: SparkSession | None = None,
        **kwargs,
    ) -> PairCol | list[PairCol]:
        return self.apply(df, engine, spark=spark, **kwargs)


class SelectColumns(BaseSparkTransform):
    op: Literal["select_columns"]
    columns: list[str]
    allow_missing_columns: bool = False

    def apply(
        self,
        df: DataFrame,
        engine: DictData,
        *,
        spark: SparkSession | None = None,
        **kwargs,
    ) -> DataFrame:
        current_cols = df.columns
        target_cols = self.columns
        if self.allow_missing_columns:
            target_cols = [c for c in self.columns if c in current_cols]
        return df.select(*target_cols)


class DropColumns(BaseSparkTransform):
    op: Literal["drop_columns"]
    columns: list[str]
    allow_missing_columns: bool = False

    def apply(
        self,
        df: DataFrame,
        engine: DictData,
        *,
        spark: SparkSession | None = None,
        **kwargs,
    ) -> DataFrame:
        current_cols = df.columns
        target_cols = self.columns
        if self.allow_missing_columns:
            target_cols = [c for c in self.columns if c in current_cols]
        return df.drop(*target_cols)


class CleanMongoJsonStr(BaseSparkTransform):
    op: Literal["clean_mongo_json_string"]
    source: str
    use_java: bool = True

    def apply(
        self,
        df: DataFrame,
        engine: DictData,
        *,
        spark: SparkSession | None = None,
        **kwargs,
    ) -> PairCol:
        """Apply to Clean Mongo Json string."""
        from ..udf import clean_mongo_json_udf

        func_name: str = "cleanMongoJsonString"
        if self.use_java:
            java_class_name: str = "dap.CleanMongoJsonStringUDF"
            spark.udf.registerJavaFunction(
                func_name, java_class_name, StringType()
            )
            column: Column = expr(f"{func_name}({self.source})").cast("string")
            return column, self.source
        mongo_udf = clean_mongo_json_udf(spark)
        return mongo_udf(col(self.source)), self.source


class JsonStrToStruct(BaseSparkTransform):
    op: Literal["json_str_to_struct"]
    source: str
    infer_timestamp: bool = True
    timestamp_format: str = "yyyy-MM-dd'T'HH:mm:ss[.SSS]'Z'"
    timestampntz_format: str = "yyyy-MM-dd'T'HH:mm:ss[.SSS]'Z'"
    mode: Literal["PERMISSIVE", "DROPMALFORMED", "FAILFAST"] = "FAILFAST"
    tmp_dir: str | None = None

    def override_tmp_dir(self) -> str:
        # NOTE: spark connect, cannot use RDD, so write to temporary instead
        # TODO: need to add post execute func - to clean up temporary directory
        rand_str: str = get_random_str_unique(n=12)
        current_date: date = get_dt_now().date()
        return f"{self.tmp_dir}/json_string_to_struct/{current_date}/{rand_str}"

    def apply(
        self,
        df: DataFrame,
        engine: DictData,
        *,
        spark: SparkSession | None = None,
        **kwargs,
    ) -> AnyApplyOutput:
        """Apply convert JSON string column into new pyspark dataframe."""
        if is_remote_session(spark):
            temp_dir: str = self.override_tmp_dir()
            logger.info(
                "[func json_string_to_struct] write data to temporary dir: %s",
                temp_dir,
            )
            (
                df.select(self.source)
                .write.format("text")
                .mode("overwrite")
                .save(temp_dir)
            )
            df_reader = spark.read.format("json").option("inferSchema", True)
            if not self.infer_timestamp:
                return df_reader.load(temp_dir, mode=self.mode)
            return (
                df_reader.option("inferTimestamp", self.infer_timestamp)
                .option("timestampNTZFormat", self.timestampntz_format)
                .load(
                    temp_dir,
                    timestampFormat=self.timestamp_format,
                    mode=self.mode,
                )
            )

        # NOTE: Cannot do c[str(self.source)] in lambda func, it will raise error
        source_str: str = str(self.source)
        rdd_data = df.rdd.map(lambda c: c[source_str])
        df_reader = spark.read.option("inferSchema", True)
        if self.infer_timestamp:
            return (
                df_reader.option("inferTimestamp", self.infer_timestamp)
                .option("timestampNTZFormat", self.timestampntz_format)
                .json(
                    rdd_data,
                    timestampFormat=self.timestamp_format,
                    mode=self.mode,
                )
            )
        return df_reader.json(rdd_data, mode=self.mode)


class Scd2(BaseSparkTransform):
    op: Literal["scd2"]
    merge_key: list[str]
    update_key: str
    create_key: str
    col_start_name: str = "_scd2_start_time"
    col_end_name: str = "_scd2_end_time"

    @field_validator(
        "merge_key", mode="before", json_schema_input_type=list[str] | str
    )
    def __convert_merge_key_to_dict(cls, value: Any) -> Any:
        """Convert the `merge_key` column that pass with string type to list."""
        if isinstance(value, str):
            return [value]
        return value

    def apply(
        self,
        df: DataFrame,
        engine: DictData,
        *,
        spark: SparkSession | None = None,
        **kwargs,
    ) -> AnyApplyOutput:
        """Apply to SCD2.

        Notes:
            A method to prepare scd type 2 data. It has 4 steps:.
                1: get df_target from data warehouse (if exists)
                2: add `_scd2_start_time` and `_scd_end_time` column to df_source
                    - `_scd2_start_time`:
                        - if id of the row already exist in target table, _scd2_start_time will be update_key for that row
                        - if id of the row doesn't exist in target table (new record), _scd2_start_time will be create_key for that row
                    - `_scd2_end_time`: Defaults to NULL
                3: update df_target._scd_end_time from null to df_source._scd2_start_time in some rows
                4: combine 2 df from step 2 and 3 together and send it to sink process

        Args:
            df: source dataframe (incoming data)
            engine:
            spark:

        Returns:
            Dataframe: the final dataframe that ready to write to the table
        """
        db_name: str | None = None
        table_name: str | None = None
        # NOTE: check if config_dict is not empty (not {})
        _sink: "Sink" = engine["engine"].sink
        if _sink.type in ("iceberg-hdfs",):
            logger.info("use sink's configuration from config dict")
            db_name = _sink.database
            table_name = _sink.table_name
        elif _sink.type.startswith("local"):
            raise NotImplementedError(
                "Not support sink with local file sink type."
            )
        else:
            logger.info(
                "no db_name and table_name provided. so no need to connect to "
                "target table and just add the _scd2_start_time and "
                "_scd2_end_time to source df"
            )
        if (
            db_name
            and table_name
            and is_table_exist(
                spark=spark,
                database=db_name,
                table_name=table_name,
            )
        ):
            # --- step 1: get df_tgt
            tgt_df = spark.sql(f"SELECT * FROM {db_name}.{table_name}").filter(
                col(self.col_end_name).isNull()
            )
            # --- step 2: add _scd2_start_time and _scd_end_time column to df_source
            src_df = self._add_start_and_end_in_src(tgt_df=tgt_df, src_df=df)

            # --- step 3: update df_tgt._scd_end_time from null to df_source._scd2_start_time
            rs_df = self._update_end_time_in_tgt(tgt_df=tgt_df, src_df=src_df)

            # --- step 4: combine 2 df together
            return rs_df.unionByName(src_df, allowMissingColumns=True)

        # NOTE: if target table doesn't exist or db_name and table name are not
        #   provided, choose created_at as {scd2_start_col}.
        return df.withColumns(
            {
                self.col_start_name: col(self.create_key),
                self.col_end_name: lit(None).cast("timestamp"),
            }
        )

    def _add_start_and_end_in_src(
        self,
        tgt_df: DataFrame,
        src_df: DataFrame,
    ) -> DataFrame:
        """Add _scd2_start_time and _scd_end_time column to df_source"""
        # get existing id in target table and max updated_at value
        df_t_id = tgt_df.groupby(self.merge_key).agg(
            spark_max(self.update_key).alias("_max_update_key")
        )
        df_s_merge = src_df.join(df_t_id, on=self.merge_key, how="left")

        # persist it before checking the wrong updated_at value to prevent recompute data
        df_s_merge.persist()

        # check the wrong updated_at value from source
        # if target['_max_update_key'] > source[update_key]: it should not happen, we cannot get the past data from source.
        select_col: list[str] = self.merge_key + [
            self.update_key,
            "_max_update_key",
        ]
        condition: Column = col("_max_update_key") > col(self.update_key)
        wrong_df: DataFrame = df_s_merge.where(condition).select(*select_col)
        wrong_data: list[Row] = wrong_df.take(1)
        # NOTE: it's already persisted so should not take that much time to
        #   execute take(1)
        if len(wrong_data) > 0:
            raise ValueError(
                f"There are wrong `{self.update_key}` from source. It's lower "
                f"than the existing value in the target table.",
                f"This is the first row of the wrong data -> {wrong_data[0]}",
            )

        # if target['_max_update_key'] = source[update_key]: this case will happen when that rows already exist in the target because of rerunning the same job. So no need to reprocess and save it again.
        df_s_merge_filter = df_s_merge.filter(
            (df_t_id["_max_update_key"] < src_df[self.update_key])
            | (df_t_id["_max_update_key"].isNull())
        )

        if df_s_merge_filter.isEmpty():
            logger.info(
                "Every row in source df already existed in the target table. "
                "So you will get an empty dataframe result"
            )

        # create scd2_start_col by checking join result
        check_merge_not_null = [
            df_t_id[key].isNotNull() for key in self.merge_key
        ]
        df_result = df_s_merge_filter.withColumns(
            {
                self.col_start_name: when(
                    *check_merge_not_null, src_df[self.update_key]
                ).otherwise(src_df[self.create_key]),
                self.col_end_name: lit(None).cast("timestamp"),
            }
        )

        # NOTE: remove merge key and _max_update_key that getting from join
        return df_result.select(
            src_df["*"], self.col_start_name, self.col_end_name
        )

    def _update_end_time_in_tgt(
        self,
        tgt_df: DataFrame,
        src_df: DataFrame,
    ) -> DataFrame:
        """Update df_target._scd_end_time from null to df_source._scd2_start_time."""
        merge_condition = [
            tgt_df[key] == src_df[key] for key in self.merge_key
        ] + [tgt_df[self.update_key] < src_df[self.update_key]]

        # NOTE: select only necessary using withColumn because we have
        #   duplicated column names
        df_result: DataFrame = tgt_df.join(
            src_df, on=merge_condition, how="inner"
        ).select(tgt_df["*"], src_df[self.col_start_name])

        # NOTE: update tgt_df[scd2_end_col] = src_df[scd2_start_col], then keep o
        #   nly target and new `_scd_end_time` column
        return df_result.withColumn(
            self.col_end_name, src_df[self.col_start_name]
        ).drop(src_df[self.col_start_name])


class ExplodeArrayColumn(BaseSparkTransform):
    op: Literal["explode_array_column"]
    explode_col: str
    is_explode_outer: bool = True
    is_return_position: bool = False
    position_prefix_name: str = "_index_pos"

    @field_validator("explode_col", "position_prefix_name", mode="after")
    def __validate_explode_col(cls, value: str) -> str:
        if "." in value:
            raise ValueError("Do not pass `.`, it supports only first level.")
        return value

    def apply(
        self,
        df: DataFrame,
        engine: DictData,
        *,
        spark: SparkSession | None = None,
        **kwargs,
    ) -> AnyApplyOutput:
        """Apply to Explode Array Column"""
        position_col_name = f"{self.position_prefix_name}_{self.explode_col}"
        col_type_obj = df.schema[self.explode_col].dataType
        if col_type_obj.typeName() not in ("array",):
            raise ValueError("Support only ('array', ) data type")

        if position_col_name in df.schema.fieldNames():
            raise ValueError(
                "Position column name is duplicated, please reconfigure "
                "`position_prefix_name`"
            )

        logger.info("explode array column")
        if not self.is_return_position:
            if self.is_explode_outer:
                return df.withColumn(
                    self.explode_col, explode_outer(col(self.explode_col))
                )
            return df.withColumn(
                self.explode_col, explode(col(self.explode_col))
            )

        cols_name = df.columns
        select_cols = [
            col_name
            for col_name in cols_name
            if not col_name == self.explode_col
        ]

        if self.is_explode_outer:
            return df.select(
                *select_cols,
                posexplode_outer(self.explode_col).alias(
                    position_col_name, self.explode_col
                ),
            )
        return df.select(
            *select_cols,
            posexplode(self.explode_col).alias(
                position_col_name, self.explode_col
            ),
        )


class FlattenAllExceptArray(BaseSparkTransform):
    op: Literal["flatten_all_columns_except_array"]

    def apply(
        self,
        df: DataFrame,
        engine: DictData,
        *,
        spark: SparkSession | None = None,
        **kwargs,
    ) -> DataFrame:
        transform_dict: dict[str, Column] = {}
        for c in extract_columns_without_array(schema=df.schema):
            flatten_col = "_".join(c.split("."))
            transform_dict[flatten_col] = col(c)

        logger.info("flatten all columns except array")
        for k, v in transform_dict.items():
            logger.info("target col: %s, from: %s", k, v)
        return df.withColumns(transform_dict).select(
            *list(transform_dict.keys())
        )
