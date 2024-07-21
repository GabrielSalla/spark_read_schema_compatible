import pytest
import re
from src.read_schema_compatible import list_path_files


@pytest.mark.parametrize("path, file_format, partitions_numbers", [
    ("test_data_with_partition_avro", "avro", "[1-4]"),
    ("test_data_with_partition_parquet", "parquet", "[1-6]"),
])
def test_list_path_files_with_partition_name(
        spark,
        data_folder,
        path,
        file_format,
        partitions_numbers
):
    """"list_path_files" should list all the paths correctly when the path has partitions
    with a partition_name. Example data/part=1/"""
    list_path = f"./{data_folder}/{path}"
    result = list_path_files(spark, file_format, list_path)

    path_regex_pattern = (
        rf"file:/{data_folder}/{path}/part={partitions_numbers}/part-[\d\w\-.]+.{file_format}"
    )
    for file_path in result:
        assert re.match(path_regex_pattern, file_path)


@pytest.mark.parametrize("path, file_format, partitions_numbers", [
    ("test_data_without_partition_avro", "avro", "[1-2]"),
    ("test_data_without_partition_parquet", "parquet", "[1-2]"),
])
def test_list_path_files_without_partition_name(
        spark,
        data_folder,
        path,
        file_format,
        partitions_numbers
):
    """"list_path_files" should list all the folders correctly when the path has partitions
    without a partition_name. Example data/1/"""
    list_path = f"./{data_folder}/{path}"
    result = list_path_files(spark, file_format, list_path)

    path_regex_pattern = (
        rf"file:/{data_folder}/{path}/{partitions_numbers}/part-[\d\w\-.]+.{file_format}"
    )
    for file_path in result:
        assert re.match(path_regex_pattern, file_path)
