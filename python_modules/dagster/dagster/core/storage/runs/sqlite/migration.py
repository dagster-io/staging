def get_sqlite_alembic_revision_from_dagster_version(dagster_version=None):
    if not dagster_version or dagster_version < [0, 7, 0]:
        return "base"

    if dagster_version < [0, 7, 7]:
        return "9fe9e746268c"  # 0.7.0

    if dagster_version < [0, 9, 21]:
        return "c63a27054f08"  # 0.7.7

    if dagster_version < [0, 10, 0]:
        return "224640159acf"  # 0.9.21

    if dagster_version < [0, 10, 7]:
        return "0da417ae1b81"  # 0.10.0

    if dagster_version < [0, 11, 0]:
        return "521d4caca7ad"  # 0.10.7

    return "72686963a802"  # 0.11.0, head