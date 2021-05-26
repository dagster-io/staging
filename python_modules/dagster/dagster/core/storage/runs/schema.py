import sqlalchemy as db

from ..sql import MySQLCompatabilityTypes, get_current_timestamp

RunStorageSqlMetadata = db.MetaData()

RunsTable = db.Table(
    "runs",
    RunStorageSqlMetadata,
    db.Column("id", db.Integer, primary_key=True, autoincrement=True),
    db.Column("run_id", db.String(255), unique=True),
    db.Column(
        "snapshot_id",
        db.String(255),
        db.ForeignKey("snapshots.snapshot_id", name="fk_runs_snapshot_id_snapshots_snapshot_id"),
    ),
    db.Column("pipeline_name", db.Text),
    db.Column("status", db.String(63)),
    db.Column("run_body", db.Text),
    db.Column("partition", db.Text),
    db.Column("partition_set", db.Text),
    db.Column("create_timestamp", db.DateTime, server_default=get_current_timestamp()),
    db.Column("update_timestamp", db.DateTime, server_default=get_current_timestamp()),
    db.Column("deployment_id", db.Integer),
)

# Secondary Index migration table, used to track data migrations, both for event_logs and runs.
# This schema should match the schema in the event_log storage schema
SecondaryIndexMigrationTable = db.Table(
    "secondary_indexes",
    RunStorageSqlMetadata,
    db.Column("id", db.Integer, primary_key=True, autoincrement=True),
    db.Column("name", MySQLCompatabilityTypes.UniqueText, unique=True),
    db.Column("create_timestamp", db.DateTime, server_default=get_current_timestamp()),
    db.Column("migration_completed", db.DateTime),
    db.Column("deployment_id", db.Integer),
)

RunTagsTable = db.Table(
    "run_tags",
    RunStorageSqlMetadata,
    db.Column("id", db.Integer, primary_key=True, autoincrement=True),
    db.Column("run_id", None, db.ForeignKey("runs.run_id", ondelete="CASCADE")),
    db.Column("key", db.Text),
    db.Column("value", db.Text),
    db.Column("deployment_id", db.Integer),
)

SnapshotsTable = db.Table(
    "snapshots",
    RunStorageSqlMetadata,
    db.Column("id", db.Integer, primary_key=True, autoincrement=True, nullable=False),
    db.Column("snapshot_id", db.String(255), unique=True, nullable=False),
    db.Column("snapshot_body", db.LargeBinary, nullable=False),
    db.Column("snapshot_type", db.String(63), nullable=False),
    db.Column("deployment_id", db.Integer),
)

DaemonHeartbeatsTable = db.Table(
    "daemon_heartbeats",
    RunStorageSqlMetadata,
    db.Column("daemon_type", db.String(255), unique=True, nullable=False),
    db.Column("daemon_id", db.String(255)),
    db.Column("timestamp", db.types.TIMESTAMP, nullable=False),
    db.Column("body", db.Text),  # serialized DaemonHeartbeat
    db.Column("deployment_id", db.Integer),
)

BulkActionsTable = db.Table(
    "bulk_actions",
    RunStorageSqlMetadata,
    db.Column("id", db.Integer, primary_key=True, autoincrement=True),
    db.Column("key", db.String(32), unique=True, nullable=False),
    db.Column("status", db.String(255), nullable=False),
    db.Column("timestamp", db.types.TIMESTAMP, nullable=False),
    db.Column("body", db.Text),
    db.Column("deployment_id", db.Integer),
)

db.Index("idx_run_tags", RunTagsTable.c.key, RunTagsTable.c.value)
db.Index("idx_run_partitions", RunsTable.c.partition_set, RunsTable.c.partition)
db.Index("idx_bulk_actions", BulkActionsTable.c.key)
db.Index("idx_bulk_actions_status", BulkActionsTable.c.status)
db.Index("idx_run_status", RunsTable.c.status)


db.Index("idx_deployment_id_runs", RunsTable.c.deployment_id)
db.Index("idx_deployment_id_run_tags", RunTagsTable.c.deployment_id)
db.Index("idx_deployment_id_snapshots", SnapshotsTable.c.deployment_id)
db.Index("idx_deployment_id_daemon_heartbeats", DaemonHeartbeatsTable.c.deployment_id)
db.Index("idx_deployment_id_bulk_actions", BulkActionsTable.c.deployment_id)