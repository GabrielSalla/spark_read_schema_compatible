import pytest
import re
from src.read_schema_compatible import get_last_file_path


@pytest.mark.parametrize("path, partition, file_format, expected_regex_pattern", [
    ("test_data_with_partition_avro", None, "avro", r"part=4"),
    ("test_data_with_partition_avro", "/part={2,3}", "avro", r"part=3"),
    ("test_data_with_partition_parquet", None, "parquet", r"part=6"),
    ("test_data_with_partition_parquet", "/part={3,4,5}", "parquet", r"part=5"),
    ("test_data_without_partition_avro", None, "avro", r"2"),
    ("test_data_without_partition_parquet", None, "parquet", r"2"),
])
def test_get_last_file_path(
        spark,
        data_folder,
        path,
        partition,
        file_format,
        expected_regex_pattern
):
    """"test_get_last_file_path" should return the path of the last file, sorting alphabetically, in
    the provided path"""
    files_path = f"{data_folder}/{path}" + (partition if partition else "")
    result = get_last_file_path(spark, file_format, files_path)
    path_regex_pattern = (
        rf"file:/{data_folder}/{path}/{expected_regex_pattern}/part-[\d\w\-.]+.{file_format}"
    )
    assert re.match(path_regex_pattern, result)


@pytest.mark.parametrize("path, partition, file_format, expected_regex_pattern", [
    ("test_data_mixed_schemas_parquet", None, "parquet", r"part=1"),
    ("test_data_mixed_schemas_parquet", "/part=1", "parquet", r"part=1"),
])
def test_get_last_file_path_s3(
        spark,
        data_folder,
        path,
        partition,
        file_format,
        expected_regex_pattern
):
    """"test_get_last_file_path" should return the path of the last file, sorting alphabetically, in
    the provided path"""
    files_path = f"s3a://{data_folder}/{path}" + (partition if partition else "")
    result = get_last_file_path(spark, file_format, files_path)
    path_regex_pattern = (
        rf"s3a://{data_folder}/{path}/{expected_regex_pattern}/3_part-[\d\w\-.]+.{file_format}"
    )
    assert re.match(path_regex_pattern, result)


def test_get_last_file_path_file_not_found(spark):
    """"test_get_last_file_path" should return None when no files are found in the provided path"""
    result = get_last_file_path(spark, "parquet", "some_path")
    assert result is None
