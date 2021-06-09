def get_sqlite_alembic_revision_from_dagster_version(dagster_version=None):
    if not dagster_version or dagster_version < [0, 7, 0]:
        return "base"

    if dagster_version < [0, 8, 0]:
        return "da7cd32b690d"  # 0.7.0

    if dagster_version < [0, 10, 0]:
        return "b22f16781a7c"  # 0.8.0

    return "0da417ae1b81"  # 0.10.0
