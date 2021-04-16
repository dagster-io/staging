import subprocess
from base64 import b64decode

import pytest
from kubernetes.client import models
from schema.charts.dagster.subschema.global_ import Global, PostgreSQLConnectionString
from schema.charts.dagster.values import DagsterHelmValues

from .helm_template import HelmTemplate


@pytest.fixture(name="template")
def helm_template() -> HelmTemplate:
    return HelmTemplate(
        output="templates/secret-postgres-connection-string.yaml",
        model=models.V1Secret,
    )


def test_postgresql_connection_string_secret_does_not_render(template: HelmTemplate, capsys):
    with pytest.raises(subprocess.CalledProcessError):
        helm_values = DagsterHelmValues.construct(
            global_=Global.construct(
                postgresqlConnectionString=PostgreSQLConnectionString.construct(
                    generatePostgresqlConnectionStringSecret=False
                )
            )
        )

        template.render(helm_values)

        _, err = capsys.readouterr()
        assert "Error: could not find template" in err


def test_postgresql_connection_string_secret_renders(template: HelmTemplate):
    secret_name = "a-secret-name"
    connection_string = "postgresql://localhost/mydb"
    helm_values = DagsterHelmValues.construct(
        global_=Global.construct(
            postgresqlConnectionString=PostgreSQLConnectionString.construct(
                postgresqlConnectionStringSecretName=secret_name,
                generatePostgresqlConnectionStringSecret=True,
                connectionString=connection_string,
            )
        )
    )

    secrets = template.render(helm_values)

    assert len(secrets) == 1

    postgres_connection_string_secret = secrets[0]

    assert postgres_connection_string_secret.metadata.name == secret_name
    assert (
        b64decode(postgres_connection_string_secret.data["postgresql-connection-string"]).decode()
        == connection_string
    )
