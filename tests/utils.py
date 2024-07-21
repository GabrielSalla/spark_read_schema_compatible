from src.utils import column

from pyspark.errors.exceptions.base import PySparkAssertionError
from pyspark.sql import DataFrame
from pyspark.testing.utils import assertDataFrameEqual


def sort_df_columns(df: DataFrame) -> DataFrame:
    """"sort_df_columns" will sort a df by its columns. We need this function
    because two DataFrames with same content and different column orders
    are considered different"""
    return df.select([column(column_name) for column_name in sorted(df.columns)])


def are_dataframes_equal(df1: DataFrame, df2: DataFrame) -> bool:
    """Check if 2 Spark dataframes are equal to each other, even if they have duplicate rows"""
    df1 = sort_df_columns(df1)
    df2 = sort_df_columns(df2)

    try:
        assertDataFrameEqual(df1, df2)
        return True
    except PySparkAssertionError:
        return False
