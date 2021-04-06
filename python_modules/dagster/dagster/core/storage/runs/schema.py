import sqlalchemy as db

from ..sql import MySQLCompatabilityTypes, get_current_timestamp

RunStorageSqlMetadata = db.MetaData()


def get_runs_table(metadata):
    table = db.Table(
        "runs",
        metadata,
        db.Column("id", db.Integer, primary_key=True, autoincrement=True),
        db.Column("run_id", db.String(255), unique=True),
        db.Column(
            "snapshot_id",
            db.String(255),
            db.ForeignKey(
                "snapshots.snapshot_id", name="fk_runs_snapshot_id_snapshots_snapshot_id"
            ),
        ),
        db.Column("pipeline_name", db.Text),
        db.Column("status", db.String(63)),
        db.Column("run_body", db.Text),
        db.Column("partition", db.Text),
        db.Column("partition_set", db.Text),
        db.Column("create_timestamp", db.DateTime, server_default=get_current_timestamp()),
        db.Column("update_timestamp", db.DateTime, server_default=get_current_timestamp()),
    )
    db.Index("idx_run_status", table.c.status)
    db.Index("idx_run_partitions", table.c.partition_set, table.c.partition)
    return table


_RunsTable = get_runs_table(RunStorageSqlMetadata)

# Secondary Index migration table, used to track data migrations, both for event_logs and runs.
# This schema should match the schema in the event_log storage schema
def get_secondary_index_migration_table(metadata):
    return db.Table(
        "secondary_indexes",
        metadata,
        db.Column("id", db.Integer, primary_key=True, autoincrement=True),
        db.Column("name", MySQLCompatabilityTypes.UniqueText, unique=True),
        db.Column("create_timestamp", db.DateTime, server_default=get_current_timestamp()),
        db.Column("migration_completed", db.DateTime),
    )


_SecondaryIndexMigrationTable = get_secondary_index_migration_table(RunStorageSqlMetadata)


def get_run_tags_table(metadata):
    table = db.Table(
        "run_tags",
        metadata,
        db.Column("id", db.Integer, primary_key=True, autoincrement=True),
        db.Column("run_id", None, db.ForeignKey("runs.run_id", ondelete="CASCADE")),
        db.Column("key", db.Text),
        db.Column("value", db.Text),
    )
    db.Index("idx_run_tags", table.c.key, table.c.value)
    return table


_RunTagsTable = get_run_tags_table(RunStorageSqlMetadata)


def get_snapshots_table(metadata):
    return db.Table(
        "snapshots",
        metadata,
        db.Column("id", db.Integer, primary_key=True, autoincrement=True, nullable=False),
        db.Column("snapshot_id", db.String(255), unique=True, nullable=False),
        db.Column("snapshot_body", db.LargeBinary, nullable=False),
        db.Column("snapshot_type", db.String(63), nullable=False),
    )


_SnapshotsTable = get_snapshots_table(RunStorageSqlMetadata)


def get_daemon_heartbeats_table(metadata):
    return db.Table(
        "daemon_heartbeats",
        metadata,
        db.Column("daemon_type", db.String(255), unique=True, nullable=False),
        db.Column("daemon_id", db.String(255)),
        db.Column("timestamp", db.types.TIMESTAMP, nullable=False),
        db.Column("body", db.Text),  # serialized DaemonHeartbeat
    )


_DaemonHeartbeatsTable = get_daemon_heartbeats_table(RunStorageSqlMetadata)


def get_bulk_actions_table(metadata):
    table = db.Table(
        "bulk_actions",
        metadata,
        db.Column("id", db.Integer, primary_key=True, autoincrement=True),
        db.Column("key", db.String(32), unique=True, nullable=False),
        db.Column("status", db.String(255), nullable=False),
        db.Column("timestamp", db.types.TIMESTAMP, nullable=False),
        db.Column("body", db.Text),
    )

    db.Index("idx_bulk_actions", table.c.key)
    db.Index("idx_bulk_actions_status", table.c.status)

    return table


_BulkActionsTable = get_bulk_actions_table(RunStorageSqlMetadata)
