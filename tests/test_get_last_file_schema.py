import pytest
from src.read_schema_compatible import get_last_file_schema


@pytest.mark.parametrize("file_format", ["avro", "parquet"])
@pytest.mark.parametrize("partition", [1, 2, 3, 4])
def test_get_last_file_schema_with_partition(
        spark,
        data_folder,
        data_parts,
        file_format,
        partition
):
    result = get_last_file_schema(
        spark,
        file_format,
        f"{data_folder}/test_data_with_partition_{file_format}/part={partition}"
    )
    assert result == data_parts[partition].schema


@pytest.mark.parametrize("file_format", ["avro", "parquet"])
@pytest.mark.parametrize("partitions", [("1", "2"), ("2", "3"), ("2", "3", "4")])
def test_get_last_file_schema_with_multiple_partitions(
        spark,
        data_folder,
        data_parts,
        file_format,
        partitions
):
    partition_value = "{" + ",".join(partitions) + "}"
    result = get_last_file_schema(
        spark,
        file_format,
        f"{data_folder}/test_data_with_partition_{file_format}/part={partition_value}"
    )
    assert result == data_parts[int(partitions[-1])].schema
