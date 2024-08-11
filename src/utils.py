from pyspark.sql import DataFrame
from pyspark.sql.connect.column import Column
from pyspark.sql.functions import col
from pyspark.sql.types import StructType


def column(column_name: str) -> Column:
    """Get a Spark's Column object with the column name escaped, making it compatible with column
    names that have special characters, like dots"""
    return col(f"`{column_name}`")


def convert_dataframe_to_schema(dataframe: DataFrame, schema: StructType) -> DataFrame:
    """Cast all columns from a dataframe to the provided schema"""
    for dataframe_column in dataframe.schema:
        # Only cast the column to the type in the schema if the column exists in the schema
        # Escaped columns are necessary to reference the columns correctly, but in the schema they
        # are not escaped
        if dataframe_column.name in schema.names:
            desired_type = schema[dataframe_column.name].dataType
            dataframe = dataframe.withColumn(
                dataframe_column.name,
                column(dataframe_column.name).cast(desired_type)
            )
    return dataframe


def union_dfs(dataframes: list[DataFrame]) -> DataFrame:
    """Union a list of dataframes into a single one"""
    complete_dataframe = dataframes[0]
    for dataframe in dataframes[1:]:
        complete_dataframe = complete_dataframe.unionByName(dataframe, allowMissingColumns=True)
    return complete_dataframe
