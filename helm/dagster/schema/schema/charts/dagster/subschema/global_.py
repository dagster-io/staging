from pydantic import BaseModel  # pylint: disable=no-name-in-module


class Global(BaseModel):
    postgresqlSecretName: str
    postgresqlConnectionStringSecretName: str
    dagsterHome: str
    serviceAccountName: str
