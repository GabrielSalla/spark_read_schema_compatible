def _build_path(base_path: str, partition_name: str = None, partition_value: str = None) -> str:
    if not base_path.endswith("/"):
        base_path += "/"
    path = base_path
    if partition_name:
        if not partition_value:
            raise ValueError("Expected 'partition_name' to be used with 'partition_value'")
        path += partition_name + "="
    if partition_value:
        path += "{" + partition_value + "}"
    return path


def get_path(
        base_path: str,
        partition_name: str = None,
        patterns_list: list[str] = None
) -> str:
    """Build a complete path using partitioning and patterns, if provided"""
    if patterns_list:
        patterns = ",".join(patterns_list)
        return _build_path(base_path, partition_name, patterns)
    return _build_path(base_path, partition_name)


def get_path_patterns(
        base_path: str,
        partition_name: str = None,
        patterns_list: list[str] = None
) -> str:
    """Build a list of paths for every single pattern provided"""
    if patterns_list:
        return [
            _build_path(base_path, partition_name, pattern)
            for pattern in patterns_list
        ]
    return [_build_path(base_path, partition_name)]
