import pytest
import yaml
from kubernetes.client import models
from schema.charts.dagster.subschema.postgresql import PostgreSQLConnectionString
from schema.charts.dagster.values import DagsterHelmValues

from .helm_template import HelmTemplate


@pytest.fixture(name="template")
def helm_template() -> HelmTemplate:
    return HelmTemplate(
        output="templates/configmap-instance.yaml",
        model=models.V1ConfigMap,
    )


def test_storage_postgres_url_config(template: HelmTemplate):
    helm_values = DagsterHelmValues.construct(
        postgresqlConnectionString=PostgreSQLConnectionString.construct(enabled=True)
    )

    configmaps = template.render(helm_values)

    assert len(configmaps) == 1

    instance = yaml.full_load(configmaps[0].data["dagster.yaml"])

    assert instance["schedule_storage"]["config"]["postgres_url"]
    assert instance["run_storage"]["config"]["postgres_url"]
    assert instance["event_log_storage"]["config"]["postgres_url"]


def test_storage_postgres_db_config(template: HelmTemplate):
    helm_values = DagsterHelmValues.construct()

    configmaps = template.render(helm_values)

    assert len(configmaps) == 1

    instance = yaml.full_load(configmaps[0].data["dagster.yaml"])

    assert instance["schedule_storage"]["config"]["postgres_db"]
    assert instance["run_storage"]["config"]["postgres_db"]
    assert instance["event_log_storage"]["config"]["postgres_db"]
