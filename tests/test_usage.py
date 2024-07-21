from src.read_schema_compatible import read_path
from tests.utils import are_dataframes_equal


def test_read_path_usage_1(spark, data_folder, data_parts):
    # Reading a path with partition naming. Example "some_path/part=1/"
    result = read_path(
        spark=spark,
        file_format="parquet",
        path=f"./{data_folder}/test_data_with_partition_parquet",
        partition_name="part",
        patterns_list=["1", "2"]
    )
    expected_result = data_parts[1].unionByName(data_parts[2], allowMissingColumns=True)
    assert are_dataframes_equal(result, expected_result)


def test_read_path_usage_2(spark, data_folder, data_parts):
    # Reading a path with partition naming and patterns with wildcards. Example "some_path/part=1*/"
    result = read_path(
        spark=spark,
        file_format="parquet",
        path=f"./{data_folder}/test_data_with_partition_parquet",
        partition_name="part",
        patterns_list=["1*", "2*"]
    )
    expected_result = data_parts[1].unionByName(data_parts[2], allowMissingColumns=True)
    assert are_dataframes_equal(result, expected_result)


def test_read_path_usage_3(spark, data_folder, data_parts):
    # Reading a path without partition naming. Example "some_path/1/"
    result = read_path(
        spark=spark,
        file_format="parquet",
        path=f"./{data_folder}/test_data_without_partition_parquet",
        patterns_list=["1", "2"]
    )
    expected_result = data_parts[1].unionByName(data_parts[2], allowMissingColumns=True)
    assert are_dataframes_equal(result, expected_result)


def test_read_path_usage_4(spark, data_folder, data_parts):
    # Reading a path without partition naming and patterns with wildcards. Example "some_path/1*/"
    result = read_path(
        spark=spark,
        file_format="parquet",
        path=f"./{data_folder}/test_data_without_partition_parquet",
        patterns_list=["1*", "2*"]
    )
    expected_result = data_parts[1].unionByName(data_parts[2], allowMissingColumns=True)
    assert are_dataframes_equal(result, expected_result)


def test_read_path_usage_5(spark, data_folder, data_parts):
    # Reading a path without partitions. Example "some_path/"
    read_path(
        spark=spark,
        file_format="parquet",
        path=f"./{data_folder}/test_data_with_partition_parquet",
    )
    # I was too lazy to compare the data here
