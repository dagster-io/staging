from dagster import ModeDefinition, pipeline

from .database_resources import postgres_database, postgres_domain, sqlite_database, sqlite_domain
from .solids_with_resources import generate_table_1, generate_table_2


@pipeline(
    mode_defs=[
        ModeDefinition(
            "local_dev", resource_defs={"database": sqlite_database, "domain": sqlite_domain}
        ),
        ModeDefinition(
            "prod", resource_defs={"database": postgres_database, "domain": postgres_domain}
        ),
    ],
)
def generate_tables_pipeline():
    generate_table_1()
    generate_table_2()
