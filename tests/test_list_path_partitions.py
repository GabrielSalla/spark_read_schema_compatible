import pytest
from src.read_schema_compatible import list_path_partitions


@pytest.mark.parametrize("path, file_format, partitions_numbers", [
    ("test_data_with_partition_avro", "avro", range(1, 5)),
    ("test_data_with_partition_parquet", "parquet", range(1, 7)),
])
def test_list_path_partitions_with_partition_name(
        spark,
        data_folder,
        path,
        file_format,
        partitions_numbers
):
    """"list_path_partitions" should list all the partitions correctly when the path has partitions
    with a partition_name. Example data/part=1/"""
    list_path = f"./{data_folder}/{path}"
    result = list_path_partitions(spark, file_format, list_path)
    expected_result = [
        f"file:/{data_folder}/{path}/part={i}/"
        for i in partitions_numbers
    ]
    assert result == expected_result


def test_list_path_partitions_s3_with_partition_name(spark, data_folder):
    """"list_path_partitions" should list all the partitions correctly when the path has partitions
    with a partition_name. Example data/part=1/"""
    list_path = f"s3a://{data_folder}/test_data_mixed_schemas_parquet"
    result = list_path_partitions(spark, "parquet", list_path)
    expected_result = [f"s3a://{data_folder}/test_data_mixed_schemas_parquet/part=1/"]
    assert result == expected_result


@pytest.mark.parametrize("path, file_format, partitions_numbers", [
    ("test_data_without_partition_avro", "avro", range(1, 3)),
    ("test_data_without_partition_parquet", "parquet", range(1, 3)),
])
def test_list_path_partitions_without_partition_name(
        spark,
        data_folder,
        path,
        file_format,
        partitions_numbers
):
    """"list_path_partitions" should list all the partitions correctly when the path has partitions
    without a partition_name. Example data/1/"""
    list_path = f"./{data_folder}/{path}"
    result = list_path_partitions(spark, file_format, list_path)
    expected_result = [
        f"file:/{data_folder}/{path}/{i}/"
        for i in partitions_numbers
    ]
    assert result == expected_result
