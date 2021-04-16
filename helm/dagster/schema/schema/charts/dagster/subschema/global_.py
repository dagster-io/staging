from pydantic import BaseModel  # pylint: disable=no-name-in-module


class PostgreSQLConnectionString(BaseModel):
    enabled: bool
    postgresqlConnectionStringSecretName: str
    generatePostgresqlConnectionStringSecret: bool
    connectionString: str


class Global(BaseModel):
    postgresqlSecretName: str
    postgresqlConnectionString: PostgreSQLConnectionString
    dagsterHome: str
    serviceAccountName: str
