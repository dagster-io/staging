import dagster.core.storage.event_log.schema as event_log_schema
import dagster.core.storage.runs.schema as runs_schema
import dagster.core.storage.schedules.schema as schedules_schema
from sqlalchemy.sql.schema import Table


def check_schema_compat(schema):
    """We run this check to ensure that we don't have any schema columns that are incompatible with
    MySQL.
    """
    for name in dir(schema):
        obj = getattr(schema, name)
        if isinstance(obj, Table):
            print(name, obj)  # pylint: disable=print-call
            for column in list(obj.columns):
                print("  -", column, ": ", str(column.type))  # pylint: disable=print-call
                if str(column.type) == "VARCHAR":
                    raise Exception(
                        f"Column {str(column)} is type VARCHAR; cannot use bare db.String type as "
                        "it is incompatible with certain databases (MySQL). Use either a "
                        "fixed-length db.String(123) or db.Text instead."
                    )
            print("\n")  # pylint: disable=print-call


if __name__ == "__main__":
    for schema_module in [event_log_schema, runs_schema, schedules_schema]:
        check_schema_compat(schema_module)
