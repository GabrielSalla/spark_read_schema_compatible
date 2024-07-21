import pytest
from src.read_schema_compatible import _MERGE_SCHEMA_ERROR_MESSAGES
from pyspark.errors.exceptions.connect import SparkConnectGrpcException


@pytest.mark.parametrize("file_format", ["avro", "parquet"])
def test_spark_read_single_partition_schema_change_int_long(
        spark,
        data_folder,
        file_format
):
    """Spark read function should raise an exception when reading a single partition when the data
    has different schemas
    The difference between the schemas is a column changing from int to long"""
    path = f"s3a://{data_folder}/test_data_mixed_schemas_{file_format}/part=1"
    try:
        df = spark \
            .read \
            .option("recursiveFileLookup", True) \
            .option("mergeSchema", True) \
            .option("header", True) \
            .format(file_format) \
            .load(path)
        df.show()
    except SparkConnectGrpcException as e:
        assert any(msg in str(e) for msg in _MERGE_SCHEMA_ERROR_MESSAGES)
    else:
        # Fail the test if the exception wasn't raised
        assert False, "Exception wasn't raised when reading the files"


@pytest.mark.parametrize("file_format", ["avro", "parquet"])
def test_spark_read_multiple_partitions_schema_change_int_long(
        spark,
        data_folder,
        file_format
):
    """Spark read function should raise an exception when reading multiple partitions when the data
    has different schemas
    The difference between the schemas is a column changing from int to long"""
    path = f"./{data_folder}/test_data_with_partition_{file_format}/part={{1,2}}"
    try:
        df = spark \
            .read \
            .option("recursiveFileLookup", True) \
            .option("mergeSchema", True) \
            .option("header", True) \
            .format(file_format) \
            .load(path)
        df.show()
    except SparkConnectGrpcException as e:
        assert any(msg in str(e) for msg in _MERGE_SCHEMA_ERROR_MESSAGES)
    else:
        # Fail the test if the exception wasn't raised
        assert False, "Exception wasn't raised when reading the files"


@pytest.mark.parametrize("file_format", ["avro", "parquet"])
def test_spark_read_multiple_partitions_schema_change_int_string(
        spark,
        data_folder,
        file_format
):
    """Spark read function should raise an exception when reading multiple partitions when the data
    has different schemas
    The difference between the schemas is a column changing from int to string"""
    path = f"./{data_folder}/test_data_with_partition_{file_format}/part={{1,3}}"
    try:
        df = spark \
            .read \
            .option("recursiveFileLookup", True) \
            .option("mergeSchema", True) \
            .option("header", True) \
            .format(file_format) \
            .load(path)
        df.show()
    except SparkConnectGrpcException as e:
        assert any(msg in str(e) for msg in _MERGE_SCHEMA_ERROR_MESSAGES)
    else:
        # Fail the test if the exception wasn't raised
        assert False, "Exception wasn't raised when reading the files"
