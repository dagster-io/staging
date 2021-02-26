import subprocess

import pytest
from schema.schema import subschema
from schema.schema.values import HelmValues

from .helm_template import HelmTemplate


@pytest.fixture(name="template")
def helm_template():
    return HelmTemplate(output="templates/deployment-user.yaml")


def test_deployments_do_not_render(template: HelmTemplate, capsys):
    with pytest.raises(subprocess.CalledProcessError):
        values = HelmValues.construct(userDeployments=subschema.UserDeployments(enabled=False))
        template.render(values)

        _, err = capsys.readouterr()
        assert "Error: could not find template" in err


def test_single_deployment_render(template: HelmTemplate):
    values = HelmValues.construct(
        userDeployments=subschema.UserDeployments.construct(
            enabled=True,
            deployments=[
                subschema.user_deployments.UserDeployment(
                    name="deployment-one",
                    image=subschema.kubernetes.Image(
                        repository="repo/image-one", tag="tag1", pullPolicy="Always"
                    ),
                    dagsterApiGrpcArgs=["-m", "deployment_one"],
                    port=3030,
                )
            ],
        )
    )

    user_deployment_manifests = template.render(values)

    assert len(user_deployment_manifests) == 1


def test_multiple_deployments_render(template: HelmTemplate):
    values = HelmValues.construct(
        userDeployments=subschema.UserDeployments(
            enabled=True,
            deployments=[
                subschema.user_deployments.UserDeployment(
                    name="deployment-one",
                    image=subschema.kubernetes.Image(
                        repository="repo/image-one", tag="tag1", pullPolicy="Always"
                    ),
                    dagsterApiGrpcArgs=["-m", "deployment_one"],
                    port=3030,
                ),
                subschema.user_deployments.UserDeployment(
                    name="deployment-two",
                    image=subschema.kubernetes.Image(
                        repository="repo/image-two", tag="tag2", pullPolicy="Always"
                    ),
                    dagsterApiGrpcArgs=["-m", "deployment_two"],
                    port=3030,
                ),
            ],
        )
    )

    user_deployment_manifests = template.render(values)

    assert len(user_deployment_manifests) == 2
