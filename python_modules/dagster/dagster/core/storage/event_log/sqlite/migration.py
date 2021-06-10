def get_sqlite_alembic_revision_from_dagster_version(dagster_version=None):
    if not dagster_version or dagster_version < [0, 6, 7]:
        return "base"

    if dagster_version < [0, 7, 7]:
        return "567bc23fd1ac"

    if dagster_version < [0, 7, 10]:
        return "3b1e175a2be3"

    if dagster_version < [0, 8, 0]:
        return "c39c047fa021"

    if dagster_version < [0, 10, 0]:
        return "c34498c29964"

    if dagster_version < [0, 11, 0]:
        return "0da417ae1b81"

    return "3b529ad30626"  # 0.11.0 head
