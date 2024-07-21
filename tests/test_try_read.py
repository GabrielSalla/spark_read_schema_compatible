import pytest
import src.read_schema_compatible as read_schema_compatible
from pyspark.errors.exceptions.connect import (
    AnalysisException as ConnectAnalysisException,
    SparkConnectGrpcException,
)
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from src.read_schema_compatible import (
    _CORRUPTED_RECORDS_COLUMN,
    _FILE_NOT_FOUND_ERROR_MESSAGES,
    _MERGE_SCHEMA_ERROR_MESSAGES,
    try_read
)


@pytest.fixture(scope="session")
def base_dataframe(spark):
    # Dataframe with a sample data
    data = [
        (1, "aaa"),
        (2, "bbb"),
        (3, "ccc"),
    ]
    schema = StructType([
        StructField("int_type", IntegerType(), True),
        StructField("string_type", StringType(), True),
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture(scope="session")
def base_dataframe_corrupted_column(spark):
    # Dataframe with a sample data with the corrupted records column
    data = [
        (1, "aaa", "111"),
        (2, "bbb", "222"),
        (3, "ccc", "333"),
    ]
    schema = StructType([
        StructField("int_type", IntegerType(), True),
        StructField("string_type", StringType(), True),
        StructField(_CORRUPTED_RECORDS_COLUMN, StringType(), True),
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture(scope="function")
def mock_reader(monkeypatch, spark, base_dataframe, base_dataframe_corrupted_column):
    """Create a mock function to be used in the tests to control the read result"""
    def mock_function(exception=None, message=None, corrupted_records=False):
        class Reader:
            def load(self, path):
                if exception:
                    raise exception(message)
                if corrupted_records:
                    return base_dataframe_corrupted_column
                else:
                    return base_dataframe

        def get_reader(*args, **kwargs):
            return Reader()

        monkeypatch.setattr(read_schema_compatible, "_get_reader", get_reader)

    return mock_function


def test_try_read_success(spark, mock_reader, base_dataframe):
    """"try_read" should return a tuple with a dataframe and True as success when the data is read
    successfully"""
    mock_reader()
    result = try_read(
        spark=spark,
        file_format="parquet",
        path="some_path"
    )
    assert result == (base_dataframe, True)


def test_try_read_corrupted_records(spark, mock_reader):
    """"try_read" should raise a "ValueError" exception when the read files are corrupted"""
    mock_reader(corrupted_records=True)
    with pytest.raises(ValueError, match="Corrupted files"):
        try_read(
            spark=spark,
            file_format="parquet",
            path="some_path"
        )


@pytest.mark.parametrize("exception_message", _FILE_NOT_FOUND_ERROR_MESSAGES)
def test_try_read_files_not_found(spark, mock_reader, exception_message):
    """"try_read" should return a tuple with None as the dataframe and True as success when the path
    has no data"""
    mock_reader(exception=ConnectAnalysisException, message=exception_message)
    result = try_read(
        spark=spark,
        file_format="parquet",
        path="some_path"
    )
    assert result == (None, True)


def test_try_read_files_other_analysis_exception(spark, mock_reader):
    """"try_read" should raise an "ConnectAnalysisException" exception when this same exception was
    caught, but isn't a file not found error"""
    mock_reader(exception=ConnectAnalysisException, message="other message")
    with pytest.raises(ConnectAnalysisException):
        try_read(
            spark=spark,
            file_format="parquet",
            path="some_path"
        )


@pytest.mark.parametrize("exception_message", _MERGE_SCHEMA_ERROR_MESSAGES)
def test_try_read_files_merge_schema_error(spark, mock_reader, exception_message):
    """"try_read" should return a tuple with None as the dataframe and False as success when there's
    an error merging schema"""
    mock_reader(exception=SparkConnectGrpcException, message=exception_message)
    result = try_read(
        spark=spark,
        file_format="parquet",
        path="some_path"
    )
    assert result == (None, False)


def test_try_read_files_use_schema(spark, mock_reader):
    """"try_read" should not raise an "SparkConnectGrpcException" exception when this same exception
    # is caught, isn't a merge schema error and is suppressed"""
    mock_reader(exception=SparkConnectGrpcException, message="other message")
    result = try_read(
        spark=spark,
        file_format="parquet",
        path="some_path"
    )
    assert result == (None, False)


def test_try_read_files_other_grpc_exception_suppressed(spark, mock_reader):
    """"try_read" should not raise an "SparkConnectGrpcException" exception when this same exception
    # is caught, isn't a merge schema error and is suppressed"""
    mock_reader(exception=SparkConnectGrpcException, message="other message")
    result = try_read(
        spark=spark,
        file_format="parquet",
        path="some_path"
    )
    assert result == (None, False)


def test_try_read_files_other_grpc_exception(spark, mock_reader):
    """"try_read" should not raise an "SparkConnectGrpcException" exception when this same exception
    # is caught, isn't a merge schema error and is not suppressed"""
    mock_reader(exception=SparkConnectGrpcException, message="other message")
    with pytest.raises(SparkConnectGrpcException):
        try_read(
            spark=spark,
            file_format="parquet",
            path="some_path",
            raise_exception=True,
        )
