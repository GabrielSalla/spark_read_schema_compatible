import boto3
import datetime
import os
import pytest
import time
from _pytest.monkeypatch import MonkeyPatch
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from src.read_schema_compatible import ACCEPTED_FORMATS

AWS_ENDPOINT = "http://motoserver:5000"


@pytest.fixture(scope="session")
def monkeypatch_session():
    """Monkeypatch objet to be used in "session" scoped fixture"""
    mp = MonkeyPatch()
    yield mp
    mp.undo()


@pytest.fixture(scope="session", autouse=True)
def aws_mock(monkeypatch_session: MonkeyPatch):
    """Mock AWS credentials to use boto3"""
    monkeypatch_session.setitem(os.environ, "AWS_ACCESS_KEY_ID", "test")
    monkeypatch_session.setitem(os.environ, "AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch_session.setitem(os.environ, "AWS_SECURITY_TOKEN", "test")
    monkeypatch_session.setitem(os.environ, "AWS_SESSION_TOKEN", "test")
    monkeypatch_session.setitem(os.environ, "AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture(scope="session")
def data_parts(spark):
    """Create dataframes with sample data for the tests, organizing them in a dict"""
    dataframes = {}

    # Data part 1
    # Dataframe with the first part of the data
    data = [
        (1, "aaa", datetime.date(2022, 1, 1)),
        (2, "bbb", datetime.date(2022, 1, 2)),
        (3, "ccc", datetime.date(2022, 1, 3)),
    ]
    schema = StructType([
        StructField("int_type", IntegerType(), True),
        StructField("string_type", StringType(), True),
        StructField("date_type", DateType(), True),
    ])
    dataframes[1] = spark.createDataFrame(data, schema)

    # Data part 2
    # Schema change from int to bigint
    data = [
        (int(4e10), "ddd", datetime.date(2022, 2, 1)),
        (int(5e10), "eee", datetime.date(2022, 2, 2)),
        (int(6e10), "fff", datetime.date(2022, 2, 3)),
    ]
    schema = StructType([
        StructField("int_type", LongType(), True),
        StructField("string_type", StringType(), True),
        StructField("date_type", DateType(), True),
    ])
    dataframes[2] = spark.createDataFrame(data, schema)

    # Data part 3
    # Schema change from int to string
    data = [
        ("7g", "ggg", datetime.date(2022, 3, 1)),
        ("8h", "hhh", datetime.date(2022, 3, 2)),
        ("9i", "iii", datetime.date(2022, 3, 3)),
    ]
    schema = StructType([
        StructField("int_type", StringType(), True),
        StructField("string_type", StringType(), True),
        StructField("date_type", DateType(), True),
    ])
    dataframes[3] = spark.createDataFrame(data, schema)

    # Data part 4
    # Schema evolution new column
    # CSV is only compatible with new columns if they are added as the right most columns
    data = [
        (10, "jjj", datetime.date(2022, 4, 1), 100),
        (11, "kkk", datetime.date(2022, 4, 2), 200),
        (12, "lll", datetime.date(2022, 4, 3), 300),
    ]
    schema = StructType([
        StructField("int_type", IntegerType(), True),
        StructField("string_type", StringType(), True),
        StructField("date_type", DateType(), True),
        StructField("new_int_type", IntegerType(), True),
    ])
    dataframes[4] = spark.createDataFrame(data, schema)

    # Data part 5
    # Schema with columns with dots
    data = [
        (13, 300, "mmm", datetime.date(2022, 5, 1)),
        (14, 400, "nnn", datetime.date(2022, 5, 2)),
        (15, 500, "ooo", datetime.date(2022, 5, 3)),
    ]
    schema = StructType([
        StructField("col.int_type", IntegerType(), True),
        StructField("col.int_type2", IntegerType(), True),
        StructField("col.string_type", StringType(), True),
        StructField("col.date_type", DateType(), True),
    ])
    dataframes[5] = spark.createDataFrame(data, schema)

    # Data part 6
    # Schema with columns with dots, new and missing columns and schema change
    data = [
        (int(16e10), "600", "ppp", datetime.date(2022, 6, 1), "sss"),
        (int(17e10), "700", "qqq", datetime.date(2022, 6, 2), "ttt"),
        (int(18e10), "800", "rrr", datetime.date(2022, 6, 3), "uuu"),
    ]
    schema = StructType([
        StructField("col.int_type", LongType(), True),
        StructField("col.int_type2", StringType(), True),
        StructField("col.string_type", StringType(), True),
        StructField("col.date_type", DateType(), True),
        StructField("new.string_type", StringType(), True),
    ])
    dataframes[6] = spark.createDataFrame(data, schema)

    return dataframes


@pytest.fixture(scope="session")
def empty_data(spark):
    """Create a dataframe with no data"""
    data = []
    schema = StructType([
        StructField("int_type", IntegerType(), True),
        StructField("string_type", StringType(), True),
        StructField("date_type", DateType(), True),
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture(scope="session")
def data_folder(monkeypatch_session):
    """Create and return the name for the data bucket. It'll be a new one for each test session"""
    return f"data-{str(int(time.time()))}"


@pytest.fixture(scope="session", autouse=True)
def create_test_files_with_partition_name(data_folder, data_parts):
    """Create the files locally that uses partition name"""
    for file_format in ACCEPTED_FORMATS:
        path = f"./{data_folder}/test_data_with_partition_{file_format}"
        for part, data in data_parts.items():
            # Avro is not compatible with columns with dots in the name
            if file_format == "avro" and part >= 5:
                continue
            data.repartition(1).write.format(file_format) \
                .option("header", True) \
                .mode("overwrite") \
                .save(f"{path}/part={part}")


@pytest.fixture(scope="session", autouse=True)
def create_test_files_without_partition_name(data_folder, data_parts):
    """Create the files locally that don't use partition name"""
    for file_format in ACCEPTED_FORMATS:
        path = f"./{data_folder}/test_data_without_partition_{file_format}"
        data_parts[1].repartition(1).write.format(file_format) \
            .option("header", True) \
            .mode("overwrite") \
            .save(f"{path}/1")
        data_parts[2].repartition(1).write.format(file_format) \
            .option("header", True) \
            .mode("overwrite") \
            .save(f"{path}/2")


@pytest.fixture(scope="session", autouse=True)
def create_test_files_different_schemas_same_partition(data_folder, data_parts):
    """Create the files with different schemas in the same partition
    To make sure the last schema is "the most recent one", it's necessary to copy them "manually"
    to the correct path using boto3, as there's no way to assure the files will be correctly named
    using Spark"""
    s3_client = boto3.client("s3", endpoint_url=AWS_ENDPOINT)
    s3 = boto3.resource("s3", endpoint_url=AWS_ENDPOINT)

    bucket = s3.Bucket(data_folder)
    bucket.create()

    for file_format in ACCEPTED_FORMATS:
        path = f"s3a://{data_folder}/test_data_{file_format}"
        data_parts[1].repartition(1).write.format(file_format) \
            .option("header", True) \
            .mode("overwrite") \
            .save(f"{path}/part=1")
        data_parts[2].repartition(1).write.format(file_format) \
            .option("header", True) \
            .mode("overwrite") \
            .save(f"{path}/part=2")

        # Get the files
        files = [
            file.key
            for file in bucket.objects.filter(Prefix=f"test_data_{file_format}")
        ]
        files.sort()
        # Copy the files to the new path in a way that the files of the partition 2, which has a
        # newer schema, will be considered the latest ones
        for i, file in enumerate(files):
            if not file.endswith(file_format):
                continue
            # Only using parts 1 and 2
            if not any(part in file for part in ["part=1", "part=2"]):
                continue
            file_name = file.split("/")[-1]
            s3_client.copy_object(
                Bucket=data_folder,
                Key=f"test_data_mixed_schemas_{file_format}/part=1/{i}_{file_name}",
                CopySource={"Bucket": data_folder, "Key": file}
            )


@pytest.fixture(scope="session", autouse=True)
def create_test_files_empty_file(data_folder, empty_data):
    """Create empty files"""
    for file_format in ACCEPTED_FORMATS:
        path = f"./{data_folder}/test_data_empty_{file_format}"
        empty_data.repartition(1).write.format(file_format) \
            .option("header", True) \
            .mode("overwrite") \
            .save(f"{path}/part=1")


@pytest.fixture(scope="session")
def spark():
    """Creates a Spark session that connects to the server in another container"""
    return SparkSession.builder.remote("sc://spark-server:15002").getOrCreate()
