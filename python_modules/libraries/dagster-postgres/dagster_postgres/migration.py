def get_alembic_revision_from_dagster_version(dagster_version=None):
    if not dagster_version or dagster_version < [0, 7, 0]:
        return "base"

    if dagster_version < [0, 7, 7]:
        return "8f8dba68fd3b"

    if dagster_version < [0, 7, 10]:
        return "1ebdd7a9686f"

    if dagster_version < [0, 8, 0]:
        return "727ffe943a9f"

    if dagster_version < [0, 10, 0]:
        return "07f83cc13695"

    if dagster_version < [0, 10, 7]:
        return "4ea2b1f6f67b"

    if dagster_version < [0, 11, 0]:
        return "3e71cf573ba6"

    return "7cba9eeaaf1d"
