import json
import logging
import re

from pyspark.sql import SparkSession

from .utils import is_remote_session

logger = logging.getLogger("jute")


def _extract_table_names_from_spark_plan(items):
    """A recursive function for getting table names from a spark plan dictionary.

        For CTE and table names, it will be in the element that has
    `class=org.apache.spark.sql.catalyst.analysis.UnresolvedRelation`

    Args:
        items: dictionary or list containing spark plan data

    Returns:
        list[str]: list of result table names
    """
    tables: list[str] = []
    if isinstance(items, dict):
        if (
            items.get("class")
            == "org.apache.spark.sql.catalyst.analysis.UnresolvedRelation"
        ):
            multipart_identifier: str = items.get("multipartIdentifier", "[]")
            if multipart_identifier:
                table_name: str = ".".join(
                    multipart_identifier.strip("[]").split(", ")
                )
                tables.append(table_name)
        for value in items.values():
            if isinstance(value, (dict, list)):
                tables.extend(_extract_table_names_from_spark_plan(value))
    elif isinstance(items, list):
        for item in items:
            tables.extend(_extract_table_names_from_spark_plan(item))
    return tables


def extract_table_names_from_query(query: str) -> list[str]:
    """A function that return the inlets table name from SparkSQL query string.

    Args:
        query: SparkSQL query string

    Returns:
        list[str]: list of inlets table names
    """
    # NOTE: Got None for now, So I will use getOrCreate first
    # spark = SparkSession.getActiveSession()
    spark = SparkSession.builder.appName("sql_parser").getOrCreate()

    if is_remote_session(spark):
        logger.warning(
            "`extract_table_names_from_query` is not support for "
            "SparkConnectSession yet"
        )
        return []

    # if SparkSession, use Spark Plan to get the table names from query
    plan = spark._jsparkSession.sessionState().sqlParser().parsePlan(query)
    plan_items: dict | list = json.loads(plan.toJSON())
    plan_string = plan.toString()
    cte_match = re.match(r"CTE \[(.*?)]", plan_string)
    if cte_match:
        cte_list = [item.strip() for item in cte_match.group(1).split(",")]
    else:
        cte_list = []

    # NOTE: get table names from spark plan
    result_tables = _extract_table_names_from_spark_plan(plan_items)

    # NOTE: exclude cte from result_tables
    inlet_tables = list(set(result_tables) - set(cte_list))
    inlet_tables.sort()
    return inlet_tables
