import logging
from typing import Callable
from pyspark.errors.exceptions.connect import (
    AnalysisException as ConnectAnalysisException,
    SparkConnectGrpcException,
)
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, replace, lit
from pyspark.sql.readwriter import DataFrameReader
from pyspark.sql.types import StructType

from src.path_functions import get_path, get_path_patterns
from src.avro_schema_utils import convert_schema_to_avro
from src.utils import (
    convert_dataframe_to_schema,
    union_dfs,
)

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)

ACCEPTED_FORMATS = [
    "avro",
    "csv",
    "json",
    "parquet",
]

_CORRUPTED_RECORDS_COLUMN = "_corrupt_records"

_FILE_NOT_FOUND_ERROR_MESSAGES = [
    "PATH_NOT_FOUND",
    "FileNotFoundException",
]
_MERGE_SCHEMA_ERROR_MESSAGES = [
    "CANNOT_MERGE_SCHEMAS",
    "Failed to merge fields",
    "Cannot convert Avro type",
]


def _get_reader(
        spark: SparkSession,
        file_format: str,
        merge_schema: bool = False,
        schema: StructType = None
) -> DataFrameReader:
    """Creates an Spark DataFrame reader to be used with different configurations"""
    reader = spark.read.format(file_format)
    reader = reader.option("mergeSchema", merge_schema)
    reader = reader.option("recursiveFileLookup", True)
    reader = reader.option("pathGlobFilter", f"*.{file_format}")
    reader = reader.option("columnNameOfCorruptRecord", _CORRUPTED_RECORDS_COLUMN)

    if file_format == "avro" and schema:
        reader = reader.option("avroSchema", convert_schema_to_avro(schema))
    if file_format == "csv":
        reader = reader.option("header", True)
    if file_format in ("csv", "json"):
        reader = reader.option("inferSchema", True)
    return reader


def try_read(
        spark: SparkSession,
        file_format: str,
        path: str,
        merge_schema: bool = True,
        schema: StructType = None,
        type_check: bool = False,
        raise_exception: bool = False
) -> tuple[DataFrame, bool]:
    """Try to read a path and, return a tuple
    The first element as the Dataframe if the read was successful
    The second element is a boolean indicating if the read was successful"""
    reader = _get_reader(
        spark=spark,
        file_format=file_format,
        merge_schema=merge_schema,
        schema=schema,
    )

    try:
        df = reader.load(path)
        if type_check:
            df.first()
        if _CORRUPTED_RECORDS_COLUMN in df.columns:
            raise ValueError("Corrupted files")
        return df, True
    except ConnectAnalysisException as e:
        # If the file was not found, consider as a success
        if any(msg in str(e) for msg in _FILE_NOT_FOUND_ERROR_MESSAGES):
            return None, True
        raise e
    except SparkConnectGrpcException as e:
        # If there's a problem with merging the schema, consider as a failure
        if any(msg in str(e) for msg in _MERGE_SCHEMA_ERROR_MESSAGES):
            return None, False

        # As not all merging schema exception show a clear message, control if should raise the
        # exception or just return
        # When reading file by file, the exception should be raised
        if not raise_exception:
            return None, False

        raise e


def list_path_partitions(
        spark: SparkSession,
        file_format: str,
        path: str
) -> list:
    """Get all partitions that exists in a path"""
    df, success = try_read(spark, file_format, path, merge_schema=False, raise_exception=True)
    if not df:
        return

    # Remove the file name from the file path, remaining only the partitions
    file_path_column = replace(
        col("_metadata.file_path"),
        col("_metadata.file_name"),
        lit("")
    ).alias("partition_path")

    partitions_paths = df.select(file_path_column) \
        .distinct() \
        .sort(col("partition_path").asc()) \
        .collect()

    return [partition_path["partition_path"] for partition_path in partitions_paths]


def list_path_files(
        spark: SparkSession,
        file_format: str,
        path: str
) -> list:
    """Get all files that exists in a path"""
    df, success = try_read(spark, file_format, path, merge_schema=False, raise_exception=True)
    if not df:
        return

    file_path_column = col("_metadata.file_path")
    files_paths = df.select(file_path_column) \
        .distinct() \
        .sort(col("file_path").asc()) \
        .collect()
    return [file_path.file_path for file_path in files_paths]


def get_last_file_path(
        spark: SparkSession,
        file_format: str,
        path: str
) -> str:
    """Get the path of the last file, alphabetically, in a path"""
    logging.info("Getting last file path")
    df, success = try_read(spark, file_format, path, merge_schema=False, raise_exception=True)
    if not df:
        return

    file_path_column = col("_metadata.file_path")
    last_file = df.select(file_path_column).sort(col("file_path").desc()).first()
    if last_file:
        return last_file.file_path


def get_last_file_schema(
        spark: SparkSession,
        file_format: str,
        path: str
) -> StructType:
    """Get the schema of the last file, sorting alphabetically, in a path"""
    last_file_path = get_last_file_path(spark, file_format, path)
    if not last_file_path:
        return
    logging.info("Getting last file schema")
    last_file_df, success = try_read(
        spark, file_format, last_file_path, merge_schema=False, raise_exception=True)
    logging.info("Last file schema:")
    last_file_df.printSchema(logging.INFO)
    return last_file_df.schema


def read_with_fallback(
        spark: SparkSession,
        file_format: str,
        paths: list[str],
        data_schema: StructType = None,
        fallback: Callable = None,
        raise_exception: bool = False
) -> DataFrame:
    """Read all the provided paths, one by one. If the read fails, call a fallback function to
    handle the failed paths
    All dataframes are converted to the provided schema
    - "raise_exception" parameter is used to flag if the "SparkConnectGrpcException" should be
    raised if a merging schema error is not identified. It's main objective is, when reading Avro
    format data, allow falling back even if the exception message is not clear about the merging
    schema error"""
    dfs = []
    for path in paths:
        # Get the last file schema for the current path when using Avro, to have schema evolution
        # (new columns) and detect schema conflict between different files
        if file_format == "avro":
            read_schema = get_last_file_schema(spark, file_format, path)
        else:
            read_schema = None

        df, success = try_read(
            spark,
            file_format,
            path,
            schema=read_schema,
            type_check=True,
            raise_exception=raise_exception
        )
        # Data was read successfully or the path doesn't exists
        if success:
            # If there's a dataframe, use it
            # This validation is needed because the path may not exists
            if df:
                df = convert_dataframe_to_schema(df, data_schema)
                dfs.append(df)
        # Data wasn't read successfully, so use the fallback if available
        else:
            if fallback:
                df = fallback(spark, file_format, path, data_schema=data_schema)
                if df:
                    df = convert_dataframe_to_schema(df, data_schema)
                    dfs.append(df)
            else:
                raise ValueError(f"Unable to read the path '{path}' successfully")
    return union_dfs(dfs)


def read_by_file(
        spark: SparkSession,
        file_format: str,
        path: str,
        data_schema: StructType
) -> DataFrame:
    """Try to read all the files in a path, one by one, as a single dataframe casting all columns to
    the provided schema"""
    files_paths = list_path_files(spark, file_format, path)
    if not files_paths:
        return

    # When reading file by file, don't read with the schema, only use it to cast the data
    return read_with_fallback(spark, file_format, files_paths, data_schema, raise_exception=True)


def read_by_partitions(
        spark: SparkSession,
        file_format: str,
        path: str,
        data_schema: StructType
) -> DataFrame:
    """Try to read all the partitions in a path, one by one, as a single dataframe casting all
    columns to the provided schema
    If any of the partitions fails, fallback to reading file by file in that partition"""
    partitions_paths = list_path_partitions(spark, file_format, path)
    if not partitions_paths:
        return

    if len(partitions_paths) == 1:
        # Skip trying to read every partition if there's only one, because it was already tried
        return read_by_file(spark, file_format, partitions_paths[0], data_schema)
    else:
        return read_with_fallback(
            spark, file_format, partitions_paths, data_schema, fallback=read_by_file)


def read_by_pattern(
        spark: SparkSession,
        file_format: str,
        path_patterns: list[str],
        data_schema: StructType
) -> DataFrame:
    """Try to read all the patterns of a path, one by one, as a single dataframe casting all
    columns to the provided schema
    If any of the patterns fails, fallback to reading partition by partition in that pattern"""
    if len(path_patterns) == 1:
        # Skip trying to read every pattern if there's only one, because it was already tried
        return read_by_partitions(spark, file_format, path_patterns[0], data_schema)
    else:
        return read_with_fallback(
            spark, file_format, path_patterns, data_schema, fallback=read_by_partitions)


def read_path(
        spark: SparkSession,
        file_format: str,
        path: str,
        partition_name: str = None,
        patterns_list: list[str] = None
) -> DataFrame:
    """Read all data from a path, using fallbacks if there're any problems with the files"""
    if file_format not in ACCEPTED_FORMATS:
        raise ValueError(f"Invalid file format {file_format}")

    assert isinstance(path, str), f"'path' must be a string, got '{type(path)}'"
    if partition_name:
        assert isinstance(partition_name, str), \
            f"'partition_name' must be a string, got '{type(partition_name)}'"
    if patterns_list:
        assert isinstance(patterns_list, list), \
            f"'patterns_list' must be a list of strings, got '{type(patterns_list)}'"
        assert all([isinstance(partition, str) for partition in patterns_list]), \
            "'patterns_list' must be a list of strings"

    complete_path = get_path(
        base_path=path,
        partition_name=partition_name,
        patterns_list=patterns_list,
    )

    # Trying to read Avro files using the last file schema is necessary to identify new columns
    data_schema = get_last_file_schema(spark, file_format, complete_path)

    df, success = try_read(spark, file_format, complete_path, schema=data_schema, type_check=True)

    if success:
        return df
    else:
        # Use fallback if not successful
        path_patterns = get_path_patterns(
            base_path=path,
            partition_name=partition_name,
            patterns_list=patterns_list,
        )
        return read_by_pattern(spark, file_format, path_patterns, data_schema)
