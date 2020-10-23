import sqlalchemy

from dagster import resource


@resource(config_schema={"db_url": str})
def postgres(context):
    engine = sqlalchemy.create_engine(context.resource_config["db_url"])
    return engine
