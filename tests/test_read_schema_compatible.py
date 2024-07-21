import pytest
import src.read_schema_compatible as read_schema_compatible
from pyspark.sql.types import (
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from tests.utils import are_dataframes_equal
from src.utils import convert_dataframe_to_schema
from src.read_schema_compatible import (
    ACCEPTED_FORMATS,
    read_path,
)


# Functions to get the expected schemas for CSV and JSON formats, because they change while writing
# and reading as a number in CSV might be "int" as well as "long" or "string". Similar situations
# also happens for JSON format
def get_partition_1_expected_schema(file_format):
    if file_format == "csv":
        return StructType([
            StructField("int_type", IntegerType(), True),
            StructField("string_type", StringType(), True),
            StructField("date_type", DateType(), True),
        ])
    if file_format == "json":
        return StructType([
            StructField("int_type", LongType(), True),
            StructField("string_type", StringType(), True),
            StructField("date_type", StringType(), True),
        ])


def get_partition_2_expected_schema(file_format):
    if file_format == "csv":
        return StructType([
            StructField("int_type", LongType(), True),
            StructField("string_type", StringType(), True),
            StructField("date_type", DateType(), True),
        ])
    if file_format == "json":
        return StructType([
            StructField("int_type", LongType(), True),
            StructField("string_type", StringType(), True),
            StructField("date_type", StringType(), True),
        ])


def get_partition_3_expected_schema(file_format):
    if file_format == "csv":
        return StructType([
            StructField("int_type", StringType(), True),
            StructField("string_type", StringType(), True),
            StructField("date_type", DateType(), True),
        ])
    if file_format == "json":
        return StructType([
            StructField("int_type", StringType(), True),
            StructField("string_type", StringType(), True),
            StructField("date_type", StringType(), True),
        ])


def get_partition_1_and_4_expected_schema(file_format):
    if file_format == "csv":
        return StructType([
            # For some reason, some columns are cast to String, even though the schemas match
            StructField("int_type", StringType(), True),
            StructField("string_type", StringType(), True),
            StructField("date_type", StringType(), True),
            StructField("new_int_type", IntegerType(), True),
        ])
    if file_format == "json":
        return StructType([
            StructField("int_type", LongType(), True),
            StructField("string_type", StringType(), True),
            StructField("date_type", StringType(), True),
            StructField("new_int_type", LongType(), True),
        ])


def get_partition_5_and_6_expected_schema(file_format):
    if file_format == "csv":
        return StructType([
            StructField("col.int_type", StringType(), True),
            StructField("col.int_type2", StringType(), True),
            StructField("col.string_type", StringType(), True),
            StructField("col.date_type", StringType(), True),
            StructField("new.string_type", StringType(), True),
        ])
    if file_format == "json":
        return StructType([
            StructField("col.int_type", LongType(), True),
            StructField("col.int_type2", StringType(), True),
            StructField("col.string_type", StringType(), True),
            StructField("col.date_type", StringType(), True),
            StructField("new.string_type", StringType(), True),
        ])


@pytest.mark.parametrize("file_format", ACCEPTED_FORMATS)
def test_read_path_with_partition_name_partition_1(spark, data_folder, data_parts, file_format):
    """"read_path()" should read_path the provided path and partition correctly"""
    result = read_path(
        spark=spark,
        file_format=file_format,
        path=f"./{data_folder}/test_data_with_partition_{file_format}",
        partition_name="part",
        patterns_list=["1"]
    )

    expected_result = data_parts[1]
    if file_format in ("csv", "json"):
        expected_result = convert_dataframe_to_schema(
            expected_result,
            get_partition_1_expected_schema(file_format)
        )

    assert are_dataframes_equal(result, expected_result)


@pytest.mark.parametrize("file_format", ACCEPTED_FORMATS)
def test_read_path_with_partition_name_partition_2(spark, data_folder, data_parts, file_format):
    """"read_path()" should read_path the provided path and partition correctly"""
    result = read_path(
        spark=spark,
        file_format=file_format,
        path=f"./{data_folder}/test_data_with_partition_{file_format}",
        partition_name="part",
        patterns_list=["2"]
    )

    expected_result = data_parts[2]
    if file_format in ("csv", "json"):
        expected_result = convert_dataframe_to_schema(
            expected_result,
            get_partition_2_expected_schema(file_format)
        )

    assert are_dataframes_equal(result, expected_result)


@pytest.mark.parametrize("file_format", ACCEPTED_FORMATS)
def test_read_path_without_partition_name_partition_1(
        spark,
        data_folder,
        data_parts,
        file_format
):
    """"read_path()" should read_path the provided path and partition correctly"""
    result = read_path(
        spark=spark,
        file_format=file_format,
        path=f"./{data_folder}/test_data_without_partition_{file_format}",
        patterns_list=["1"]
    )

    expected_result = data_parts[1]
    if file_format in ("csv", "json"):
        expected_result = convert_dataframe_to_schema(
            expected_result,
            get_partition_1_expected_schema(file_format)
        )

    assert are_dataframes_equal(result, expected_result)


@pytest.mark.parametrize("file_format", ACCEPTED_FORMATS)
def test_read_path_without_partition_name_partition_2(
        spark,
        data_folder,
        data_parts,
        file_format
):
    """"read_path()" should read_path the provided path and partition correctly"""
    result = read_path(
        spark=spark,
        file_format=file_format,
        path=f"./{data_folder}/test_data_without_partition_{file_format}",
        patterns_list=["2"]
    )

    expected_result = data_parts[2]
    if file_format in ("csv", "json"):
        expected_result = convert_dataframe_to_schema(
            expected_result,
            get_partition_2_expected_schema(file_format)
        )

    assert are_dataframes_equal(result, expected_result)


@pytest.mark.parametrize("file_format", ACCEPTED_FORMATS)
def test_read_path_multiple_partitions_schema_change_int_long(
        spark,
        data_folder,
        data_parts,
        file_format
):
    """"read_path()" should read_path the provided path and partitions correctly, being resilient to
    compatible type changes like int -> long when there're files with different schemas in
    different partitions"""
    result = read_path(
        spark=spark,
        file_format=file_format,
        path=f"./{data_folder}/test_data_with_partition_{file_format}",
        partition_name="part",
        patterns_list=["1", "2"]
    )

    expected_result = data_parts[1].unionByName(data_parts[2], allowMissingColumns=True)
    if file_format in ("csv", "json"):
        expected_result = convert_dataframe_to_schema(
            expected_result,
            get_partition_2_expected_schema(file_format)
        )

    assert are_dataframes_equal(result, expected_result)


@pytest.mark.parametrize("file_format", ACCEPTED_FORMATS)
def test_read_path_single_partition_schema_change_int_long(
        spark,
        data_folder,
        data_parts,
        file_format
):
    """"read_path()" should read_path the provided path and partitions correctly, being resilient to
    compatible type changes like int -> long when there're files with different schemas in the same
    partition
    This test uses a S3 bucket because it was necessary to move files to the same partition"""
    result = read_path(
        spark=spark,
        file_format=file_format,
        path=f"s3a://{data_folder}/test_data_mixed_schemas_{file_format}",
        partition_name="part",
        patterns_list=["1"]
    )

    expected_result = data_parts[1].unionByName(data_parts[2], allowMissingColumns=True)
    if file_format in ("csv", "json"):
        expected_result = convert_dataframe_to_schema(
            expected_result,
            get_partition_2_expected_schema(file_format)
        )

    assert are_dataframes_equal(result, expected_result)


@pytest.mark.parametrize("file_format", ACCEPTED_FORMATS)
def test_read_path_multiple_partitions_schema_change_int_string(
        spark,
        data_folder,
        data_parts,
        file_format
):
    """"read_path()" should read_path the provided path and partitions correctly, being resilient to
    compatible type changes like int -> string when there're files with different schemas in
    different partitions"""
    result = read_path(
        spark=spark,
        file_format=file_format,
        path=f"./{data_folder}/test_data_with_partition_{file_format}",
        partition_name="part",
        patterns_list=["1", "3"]
    )

    expected_result = data_parts[1].unionByName(data_parts[3], allowMissingColumns=True)
    if file_format in ("csv", "json"):
        expected_result = convert_dataframe_to_schema(
            expected_result,
            get_partition_3_expected_schema(file_format)
        )

    assert are_dataframes_equal(result, expected_result)


@pytest.mark.parametrize("file_format", ACCEPTED_FORMATS)
def test_read_path_multiple_partitions_schema_change_int_long_string(
        spark,
        data_folder,
        data_parts,
        file_format
):
    """"read_path()" should read_path the provided path and partitions correctly, being resilient to
    compatible type changes like int -> string ans long -> string when there're files with different
    schemas in different partitions"""
    result = read_path(
        spark=spark,
        file_format=file_format,
        path=f"./{data_folder}/test_data_with_partition_{file_format}",
        partition_name="part",
        patterns_list=["1", "2", "3"]
    )

    expected_result = (
        data_parts[1]
        .unionByName(data_parts[2], allowMissingColumns=True)
        .unionByName(data_parts[3], allowMissingColumns=True)
    )
    if file_format in ("csv", "json"):
        expected_result = convert_dataframe_to_schema(
            expected_result,
            get_partition_3_expected_schema(file_format)
        )

    assert are_dataframes_equal(result, expected_result)


@pytest.mark.parametrize("file_format", ACCEPTED_FORMATS)
def test_read_path_multiple_partitions_schema_change_new_column(
        spark,
        data_folder,
        data_parts,
        file_format
):
    """"read_path()" should read_path the provided path and partitions correctly, being resilient to
    compatible type changes like new columns"""
    # CSV is only compatible with new columns if they are added as the right most columns
    result = read_path(
        spark=spark,
        file_format=file_format,
        path=f"./{data_folder}/test_data_with_partition_{file_format}",
        partition_name="part",
        patterns_list=["1", "4"]
    )

    expected_result = data_parts[1].unionByName(data_parts[4], allowMissingColumns=True)
    if file_format in ("csv", "json"):
        expected_result = convert_dataframe_to_schema(
            expected_result,
            get_partition_1_and_4_expected_schema(file_format)
        )

    assert are_dataframes_equal(result, expected_result)

    expected_columns = [
        "int_type",
        "string_type",
        "date_type",
        "new_int_type",
    ]
    assert all(column in result.columns for column in expected_columns)


@pytest.mark.parametrize("file_format", ACCEPTED_FORMATS)
def test_read_path_multiple_partitions_schema_change_columns_with_dots(
        spark,
        data_folder,
        data_parts,
        file_format
):
    """"read_path()" should read_path the provided path and partitions correctly, being resilient to
    compatible type changes like new and removed columns"""
    if file_format == "avro":
        pytest.skip("Avro is not compatible with columns with dots")

    result = read_path(
        spark=spark,
        file_format=file_format,
        path=f"./{data_folder}/test_data_with_partition_{file_format}",
        partition_name="part",
        patterns_list=["5", "6"]
    )

    expected_result = data_parts[5].unionByName(data_parts[6], allowMissingColumns=True)
    if file_format in ("csv", "json"):
        expected_result = convert_dataframe_to_schema(
            expected_result,
            get_partition_5_and_6_expected_schema(file_format)
        )

    assert are_dataframes_equal(result, expected_result)

    expected_columns = [
        "col.int_type",
        "col.int_type2",
        "col.string_type",
        "col.date_type",
        "new.string_type",
    ]
    assert all(column in result.columns for column in expected_columns)


@pytest.mark.parametrize("file_format", ACCEPTED_FORMATS)
def test_read_path_empty_file(spark, data_folder, file_format):
    """"read_path()" should return an empty dataframe when the partition has an empty file"""
    result = read_path(
        spark=spark,
        file_format=file_format,
        path=f"./{data_folder}/test_data_empty_{file_format}",
        partition_name="part",
        patterns_list=["1"]
    )

    assert result.isEmpty()


@pytest.mark.parametrize("file_format", ACCEPTED_FORMATS)
def test_read_path_path_not_exists(spark, data_folder, file_format):
    """"read_path()" should return an empty dataframe when the partition has an empty file"""
    result = read_path(
        spark=spark,
        file_format=file_format,
        path="test_data_missing_path",
        partition_name="part",
        patterns_list=["1"]
    )

    assert result is None


@pytest.mark.parametrize("file_format", ACCEPTED_FORMATS)
def test_read_path_corrupted_files(spark, data_folder, file_format):
    """"read_path()" should raise an exception when the files being read are
    corrupted
    Using files created during the Spark server image build"""
    if file_format == "csv":
        pytest.skip("CSV file can't be corrupted")

    with pytest.raises(Exception):
        read_path(
            spark=spark,
            file_format=file_format,
            path="/corrupted_data"
        )


@pytest.mark.parametrize("file_format", ["avro", "parquet"])
def test_read_path_read_by_pattern_2_patterns_2_partitions(
        mocker,
        spark,
        data_folder,
        data_parts,
        file_format
):
    """"read_path()" should read_path the provided path and partitions correctly, being resilient to
    compatible type changes, reading 2 partitions in 2 patterns"""
    read_with_fallback_spy = mocker.spy(read_schema_compatible, "read_with_fallback")
    read_by_pattern_spy = mocker.spy(read_schema_compatible, "read_by_pattern")
    read_by_partitions_spy = mocker.spy(read_schema_compatible, "read_by_partitions")
    read_by_file_spy = mocker.spy(read_schema_compatible, "read_by_file")

    result = read_path(
        spark=spark,
        file_format=file_format,
        path=f"./{data_folder}/test_data_with_partition_{file_format}",
        partition_name="part",
        patterns_list=["1", "2"]
    )
    result.show()

    expected_result = data_parts[1].unionByName(data_parts[2], allowMissingColumns=True)

    assert are_dataframes_equal(result, expected_result)

    # As avro format loading is different from parquet, ignore the call counts for it
    if file_format == "avro":
        return

    # When reading 2 partitions that have compatible data in each partition, reading by pattern
    # should be enough
    assert read_with_fallback_spy.call_count == 1

    # The 2 patterns will result in the "read_with_fallback" call with both of them,
    # without falling back
    assert read_by_pattern_spy.call_count == 1
    assert read_with_fallback_spy.call_args_list[0].args[2] == [
        f"./{data_folder}/test_data_with_partition_{file_format}/part={{1}}",
        f"./{data_folder}/test_data_with_partition_{file_format}/part={{2}}",
    ]
    assert read_with_fallback_spy.call_args_list[0].kwargs["fallback"] is \
        read_schema_compatible.read_by_partitions

    assert read_by_partitions_spy.call_count == 0
    assert read_by_file_spy.call_count == 0


@pytest.mark.parametrize("file_format", ["avro", "parquet"])
def test_read_path_read_by_partition_1_pattern_2_partitions(
        mocker,
        spark,
        data_folder,
        data_parts,
        file_format
):
    """"read_path()" should read_path the provided path and partitions correctly, being resilient to
    compatible type changes, reading 2 partitions in 1 patterns"""
    read_with_fallback_spy = mocker.spy(read_schema_compatible, "read_with_fallback")
    read_by_pattern_spy = mocker.spy(read_schema_compatible, "read_by_pattern")
    read_by_partitions_spy = mocker.spy(read_schema_compatible, "read_by_partitions")
    read_by_file_spy = mocker.spy(read_schema_compatible, "read_by_file")

    result = read_path(
        spark=spark,
        file_format=file_format,
        path=f"./{data_folder}/test_data_with_partition_{file_format}",
        partition_name="part",
        patterns_list=["[1-2]"]
    )
    result.show()

    expected_result = data_parts[1].unionByName(data_parts[2], allowMissingColumns=True)

    assert are_dataframes_equal(result, expected_result)

    # As avro format loading is different from parquet, ignore the call counts for it
    if file_format == "avro":
        return

    # When reading 2 partitions that have compatible data in each partition but as a single pattern,
    # fallback to reading by partitions
    assert read_with_fallback_spy.call_count == 1

    assert read_by_pattern_spy.call_count == 1

    # pattern [1-2] falling back to "read_by_partitions"
    assert read_by_partitions_spy.call_count == 1
    assert read_with_fallback_spy.call_args_list[0].args[2] == [
        f"file:/{data_folder}/test_data_with_partition_{file_format}/part=1/",
        f"file:/{data_folder}/test_data_with_partition_{file_format}/part=2/",
    ]
    assert read_with_fallback_spy.call_args_list[0].kwargs["fallback"] is \
        read_schema_compatible.read_by_file

    assert read_by_file_spy.call_count == 0


@pytest.mark.parametrize("file_format", ["avro", "parquet"])
def test_read_path_read_by_partition_1_pattern_3_partitions(
        mocker,
        spark,
        data_folder,
        data_parts,
        file_format
):
    """"read_path()" should read_path the provided path and partitions correctly, being resilient to
    compatible type changes, reading 2 partitions in 1 patterns"""
    read_with_fallback_spy = mocker.spy(read_schema_compatible, "read_with_fallback")
    read_by_pattern_spy = mocker.spy(read_schema_compatible, "read_by_pattern")
    read_by_partitions_spy = mocker.spy(read_schema_compatible, "read_by_partitions")
    read_by_file_spy = mocker.spy(read_schema_compatible, "read_by_file")

    result = read_path(
        spark=spark,
        file_format=file_format,
        path=f"./{data_folder}/test_data_with_partition_{file_format}",
        partition_name="part",
        patterns_list=["[1-3]"]
    )
    result.show()

    expected_result = (
        data_parts[1]
        .unionByName(data_parts[2], allowMissingColumns=True)
        .unionByName(data_parts[3], allowMissingColumns=True)
    )

    assert are_dataframes_equal(result, expected_result)

    # As avro format loading is different from parquet, ignore the call counts for it
    if file_format == "avro":
        return

    # When reading 3 partitions that have compatible data in each partition but as a single pattern,
    # fallback to reading by partitions
    assert read_with_fallback_spy.call_count == 1

    assert read_by_pattern_spy.call_count == 1

    # pattern [1-3] falling back to "read_by_partitions"
    assert read_by_partitions_spy.call_count == 1
    assert read_with_fallback_spy.call_args_list[0].args[2] == [
        f"file:/{data_folder}/test_data_with_partition_{file_format}/part=1/",
        f"file:/{data_folder}/test_data_with_partition_{file_format}/part=2/",
        f"file:/{data_folder}/test_data_with_partition_{file_format}/part=3/",
    ]
    assert read_with_fallback_spy.call_args_list[0].kwargs["fallback"] is \
        read_schema_compatible.read_by_file

    assert read_by_file_spy.call_count == 0


@pytest.mark.parametrize("file_format", ["avro", "parquet"])
def test_read_path_read_by_partition_2_patterns_3_partitions_1_fallback(
        mocker,
        spark,
        data_folder,
        data_parts,
        file_format
):
    """"read_path()" should read_path the provided path and partitions correctly, being resilient to
    compatible type changes, reading 3 partitions in 2 patterns"""
    read_with_fallback_spy = mocker.spy(read_schema_compatible, "read_with_fallback")
    read_by_pattern_spy = mocker.spy(read_schema_compatible, "read_by_pattern")
    read_by_partitions_spy = mocker.spy(read_schema_compatible, "read_by_partitions")
    read_by_file_spy = mocker.spy(read_schema_compatible, "read_by_file")

    result = read_path(
        spark=spark,
        file_format=file_format,
        path=f"./{data_folder}/test_data_with_partition_{file_format}",
        partition_name="part",
        patterns_list=["[1-2]", "3"]
    )
    result.show()

    expected_result = (
        data_parts[1]
        .unionByName(data_parts[2], allowMissingColumns=True)
        .unionByName(data_parts[3], allowMissingColumns=True)
    )

    assert are_dataframes_equal(result, expected_result)

    # As avro format loading is different from parquet, ignore the call counts for it
    if file_format == "avro":
        return

    # When reading 3 partitions in 2 patterns that have compatible data in each partition as 2
    # patterns, fallback to reading by partitions
    assert read_with_fallback_spy.call_count == 2

    # The 2 patterns will result in the "read_with_fallback" call falling back to
    # "read_by_partitions"
    assert read_by_pattern_spy.call_count == 1
    assert read_with_fallback_spy.call_args_list[0].args[2] == [
        f"./{data_folder}/test_data_with_partition_{file_format}/part={{[1-2]}}",
        f"./{data_folder}/test_data_with_partition_{file_format}/part={{3}}",
    ]
    assert read_with_fallback_spy.call_args_list[0].kwargs["fallback"] is \
        read_schema_compatible.read_by_partitions

    # pattern [1-2] falling back to "read_by_partitions"
    assert read_by_partitions_spy.call_count == 1
    assert read_with_fallback_spy.call_args_list[1].args[2] == [
        f"file:/{data_folder}/test_data_with_partition_{file_format}/part=1/",
        f"file:/{data_folder}/test_data_with_partition_{file_format}/part=2/",
    ]
    assert read_with_fallback_spy.call_args_list[1].kwargs["fallback"] is \
        read_schema_compatible.read_by_file

    assert read_by_file_spy.call_count == 0


@pytest.mark.parametrize("file_format", ["avro", "parquet"])
def test_read_path_read_by_partition_2_patterns_3_partitions_2_fallbacks(
        mocker,
        spark,
        data_folder,
        data_parts,
        file_format
):
    """"read_path()" should read_path the provided path and partitions correctly, being resilient to
    compatible type changes, reading 3 partitions in 2 patterns"""
    read_with_fallback_spy = mocker.spy(read_schema_compatible, "read_with_fallback")
    read_by_pattern_spy = mocker.spy(read_schema_compatible, "read_by_pattern")
    read_by_partitions_spy = mocker.spy(read_schema_compatible, "read_by_partitions")
    read_by_file_spy = mocker.spy(read_schema_compatible, "read_by_file")

    result = read_path(
        spark=spark,
        file_format=file_format,
        path=f"./{data_folder}/test_data_with_partition_{file_format}",
        partition_name="part",
        patterns_list=["[1-2]", "[2-3]"]
    )
    result.show()

    expected_result = (
        data_parts[1]
        .unionByName(data_parts[2], allowMissingColumns=True)
        .unionByName(data_parts[2], allowMissingColumns=True)
        .unionByName(data_parts[3], allowMissingColumns=True)
    )

    assert are_dataframes_equal(result, expected_result)

    # As avro format loading is different from parquet, ignore the call counts for it
    if file_format == "avro":
        return

    # When reading 3 partitions in 2 patterns that have compatible data in each partition as 2
    # patterns, fallback to reading by partitions
    assert read_with_fallback_spy.call_count == 3

    # The 2 patterns will result in the "read_with_fallback" call falling back to
    # "read_by_partitions"
    assert read_by_pattern_spy.call_count == 1
    assert read_with_fallback_spy.call_args_list[0].args[2] == [
        f"./{data_folder}/test_data_with_partition_{file_format}/part={{[1-2]}}",
        f"./{data_folder}/test_data_with_partition_{file_format}/part={{[2-3]}}",
    ]
    assert read_with_fallback_spy.call_args_list[0].kwargs["fallback"] is \
        read_schema_compatible.read_by_partitions

    # The pattern [1,2] will result in the "read_with_fallback" call falling back to
    # "read_by_partitions"
    assert read_by_partitions_spy.call_count == 2
    # pattern [1-2] falling back to "read_by_partitions"
    assert read_with_fallback_spy.call_args_list[1].args[2] == [
        f"file:/{data_folder}/test_data_with_partition_{file_format}/part=1/",
        f"file:/{data_folder}/test_data_with_partition_{file_format}/part=2/",
    ]
    assert read_with_fallback_spy.call_args_list[1].kwargs["fallback"] is \
        read_schema_compatible.read_by_file
    # pattern [2-3] falling back to "read_by_partitions"
    assert read_with_fallback_spy.call_args_list[2].args[2] == [
        f"file:/{data_folder}/test_data_with_partition_{file_format}/part=2/",
        f"file:/{data_folder}/test_data_with_partition_{file_format}/part=3/",
    ]
    assert read_with_fallback_spy.call_args_list[2].kwargs["fallback"] is \
        read_schema_compatible.read_by_file

    assert read_by_file_spy.call_count == 0


@pytest.mark.parametrize("file_format", ["avro", "parquet"])
def test_read_path_read_by_partition_1_pattern_1_partition(
        mocker,
        spark,
        data_folder,
        data_parts,
        file_format
):
    """"read_path()" should read_path the provided path and partitions correctly, being resilient to
    compatible type changes, reading 3 partitions in 2 patterns
    This test uses a S3 bucket because it was necessary to move files to the same partition"""
    read_with_fallback_spy = mocker.spy(read_schema_compatible, "read_with_fallback")
    read_by_pattern_spy = mocker.spy(read_schema_compatible, "read_by_pattern")
    read_by_partitions_spy = mocker.spy(read_schema_compatible, "read_by_partitions")
    read_by_file_spy = mocker.spy(read_schema_compatible, "read_by_file")

    result = read_path(
        spark=spark,
        file_format=file_format,
        path=f"s3a://{data_folder}/test_data_mixed_schemas_{file_format}",
        partition_name="part",
        patterns_list=["1"]
    )
    result.show()

    expected_result = data_parts[1].unionByName(data_parts[2], allowMissingColumns=True)

    assert are_dataframes_equal(result, expected_result)

    # As avro format loading is different from parquet, ignore the call counts for it
    if file_format == "avro":
        return

    # When reading 2 partitions that have compatible data in each partition but as a single pattern,
    # fallback to reading by partitions, without using read_with_fallback
    assert read_with_fallback_spy.call_count == 1
    assert read_by_pattern_spy.call_count == 1
    # pattern 1 falling back to "read_by_partitions"
    assert read_by_partitions_spy.call_count == 1
    # Partition 1 falling back to "read_by_file"
    assert read_by_file_spy.call_count == 1
