import sqlalchemy as db

from dagster.core.storage.event_log.sql_event_log import (
    AssetAwareSqlEventLogStorage,
    SqlEventLogStorage,
)

from .schema import AssetKeyTable, SqlEventLogStorageTable


def migrate_event_log_data(instance=None):
    """
    Utility method to migrate the data in the existing event log records.  Reads every event log row
    reachable from the instance and reserializes it to event log storage.  Deserializing and then
    reserializing the event from storage allows for things like SQL column extraction, filling
    explicit default values, etc.
    """
    event_log_storage = instance._event_storage  # pylint: disable=protected-access
    if not isinstance(event_log_storage, SqlEventLogStorage):
        return

    for run in instance.get_runs():
        event_records_by_id = event_log_storage.get_logs_for_run_by_log_id(run.run_id)
        for record_id, event in event_records_by_id.items():
            event_log_storage.update_event_log_record(record_id, event)


def migrate_asset_key_data(instance=None):
    """
    Utility method to migrate the data in the existing event log records.  Reads every event log row
    reachable from the instance and reserializes it to event log storage.  Deserializing and then
    reserializing the event from storage allows for things like SQL column extraction, filling
    explicit default values, etc.
    """
    event_log_storage = instance._event_storage  # pylint: disable=protected-access
    if not isinstance(event_log_storage, AssetAwareSqlEventLogStorage):
        return

    query = (
        db.select([SqlEventLogStorageTable.c.asset_key, db.func.count().label("count"),])
        .where(SqlEventLogStorageTable.c.asset_key != None)
        .group_by(SqlEventLogStorageTable.c.asset_key)
    )
    with event_log_storage.connect() as conn:
        to_insert = conn.execute(query).fetchall()
        for (asset_key, count) in to_insert:
            try:
                conn.execute(
                    AssetKeyTable.insert().values(  # pylint: disable=no-value-for-parameter
                        asset_key=asset_key, counter=count,
                    )
                )
            except db.exc.IntegrityError:
                # asset key already present
                conn.execute(
                    AssetKeyTable.update()  # pylint: disable=no-value-for-parameter
                    .where(AssetKeyTable.c.asset_key == asset_key)
                    .values(counter=count)
                )
