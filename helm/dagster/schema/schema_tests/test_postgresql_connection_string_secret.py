import subprocess

import pytest
from kubernetes.client import models
from schema.charts.dagster.subschema.postgresql import PostgreSQLConnectionString
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
            postgresqlConnectionString=PostgreSQLConnectionString.construct(
                enabled=True, generatePostgresqlConnectionStringSecret=False
            )
        )

        template.render(helm_values)

        _, err = capsys.readouterr()
        assert "Error: could not find template" in err


def test_postgresql_connection_string_secret_renders(template: HelmTemplate):
    helm_values = DagsterHelmValues.construct(
        postgresqlConnectionString=PostgreSQLConnectionString(
            enabled=True,
            generatePostgresqlConnectionStringSecret=True,
            connectionString="postgresql://localhost",
        )
    )

    secrets = template.render(helm_values)

    assert len(secrets) == 1
