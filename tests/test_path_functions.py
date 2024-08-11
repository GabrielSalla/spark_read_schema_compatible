import pytest

from src.path_functions import _build_path, get_path, get_path_patterns


@pytest.mark.parametrize("base_path, expected_result", [
    ("aa/bb", "aa/bb/"),
    ("aa/bb/cc/", "aa/bb/cc/"),
])
def test_init(base_path, expected_result):
    """"_build_path" should append an "/" to the base path when created, if not ending with "/"
    already"""
    path = _build_path(base_path)
    assert path == expected_result


@pytest.mark.parametrize("base_path, partition_name, pattern, expected_result", [
    ("aa/bb/", None, None, "aa/bb/"),
    ("aa/bb/cc/", None, None, "aa/bb/cc/"),
    ("aa/", "part", "1", "aa/part={1}"),
    ("aa/cc/", "partition", "1,2,3", "aa/cc/partition={1,2,3}"),
    ("aa/bb/cc/", "partition", "[1,2,3]", "aa/bb/cc/partition={[1,2,3]}"),
])
def test_build_path(base_path, partition_name, pattern, expected_result):
    """"_build_path" should return the correct path, using the "partition_name" and
    "pattern" values"""
    result = _build_path(
        base_path=base_path,
        partition_name=partition_name,
        pattern=pattern,
    )
    assert result == expected_result


@pytest.mark.parametrize("base_path, pattern, expected_result", [
    ("aa/bb/", "1", "aa/bb/{1}"),
    ("aa/bb/cc/", "1,2,3", "aa/bb/cc/{1,2,3}"),
    ("aa/bb/cc/", "[1,2,3]", "aa/bb/cc/{[1,2,3]}"),
])
def test_build_path_partition_value_without_partition_name(
        base_path,
        pattern,
        expected_result
):
    """"_build_path" should create the correct path, when "partition_name" is None but
    "pattern" is defined"""
    result = _build_path(
        base_path=base_path,
        pattern=pattern,
    )
    assert result == expected_result


@pytest.mark.parametrize("base_path, partition_name", [
    ("aa/bb", "part"),
    ("aa/bb/cc/", "partition"),
])
def test_build_path_partition_name_without_partition_value(base_path, partition_name):
    """"_build_path" should raise an "ValueError" exception when "partition_name" is used without a
    "pattern" """
    with pytest.raises(
        ValueError,
        match="Expected 'partition_name' to be used with 'pattern'"
    ):
        _build_path(
            base_path=base_path,
            partition_name=partition_name,
        )


@pytest.mark.parametrize("base_path, partition_name, patterns_list, expected_result", [
    ("aa/bb", None, None, "aa/bb/"),
    ("aa/bb/cc/", None, None, "aa/bb/cc/"),
    ("aa/", "part", ["1"], "aa/part={1}"),
    ("aa/cc", "partition", ["1", "2", "3"], "aa/cc/partition={1,2,3}"),
    ("aa/", None, ["1"], "aa/{1}"),
    ("aa/cc", None, ["1", "2", "3"], "aa/cc/{1,2,3}"),
    ("aa/bb/cc", None, ["[1,2,3]"], "aa/bb/cc/{[1,2,3]}"),
])
def test_get_path(base_path, partition_name, patterns_list, expected_result):
    """"get_path" should return the correct path, using the "partition_name" and
    "partition_value" values"""
    result = get_path(
        base_path=base_path,
        partition_name=partition_name,
        patterns_list=patterns_list,
    )
    assert result == expected_result


@pytest.mark.parametrize("base_path, partition_name, patterns_list, expected_result", [
    ("aa/bb", None, None, ["aa/bb/"]),
    ("aa/bb/cc/", None, None, ["aa/bb/cc/"]),
    ("aa/", "part", ["1"], ["aa/part={1}"]),
    ("aa/cc", "partition", ["1", "2", "3"], [f"aa/cc/partition={{{i}}}" for i in range(1, 4)]),
    ("aa/", None, ["1"], ["aa/{1}"]),
    ("aa/cc", None, ["1", "2", "3"], [f"aa/cc/{{{i}}}" for i in range(1, 4)]),
    ("aa/", "part", ["[1,2,3]"], ["aa/part={[1,2,3]}"]),
])
def test_get_path_patterns(base_path, partition_name, patterns_list, expected_result):
    """"get_path_patterns" should return a list with the correct paths, using the "partition_name"
    and "partition_value" values, being 1 element for each value in "patterns_list" """
    result = get_path_patterns(
        base_path=base_path,
        partition_name=partition_name,
        patterns_list=patterns_list,
    )
    assert result == expected_result
