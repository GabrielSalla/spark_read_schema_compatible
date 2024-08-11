def _build_path(base_path: str, partition_name: str = None, pattern: str = None) -> str:
    """Build the path to be read based on a base path, a partition name and a pattern"""
    # Assert the base path ends with an "/"
    if not base_path.endswith("/"):
        base_path += "/"
    path = base_path
    if partition_name:
        if not pattern:
            raise ValueError("Expected 'partition_name' to be used with 'pattern'")
        path += partition_name + "="
    if pattern:
        path += "{" + pattern + "}"
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
