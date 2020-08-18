import json
from collections import namedtuple

from dagster import check
from dagster.config.field_utils import Permissive, Shape
from dagster.config.validate import validate_config
from dagster.core.errors import DagsterInvalidConfigError, DagsterInvariantViolationError
from dagster.utils import frozentags
from dagster.utils.merger import merge_dicts

K8S_RESOURCE_REQUIREMENTS_KEY = 'dagster-k8s/resource_requirements'
K8S_RESOURCE_REQUIREMENTS_SCHEMA = Shape({'limits': Permissive(), 'requests': Permissive()})

K8S_CONFIG_KEY = 'dagster-k8s/config'
K8S_CONFIG_SCHEMA = Shape(
    {
        'container_config': Permissive(),
        'container_config': Permissive(),
        'pod_spec_config': Permissive(),
        'job_config': Permissive(),
        'job_spec_config': Permissive(),
    }
)


class DagsterK8sConfig(
    namedtuple(
        'DagsterK8sConfig',
        'container_config pod_template_spec_config pod_spec_config job_config job_spec_config',
    )
):
    def __new__(
        cls,
        container_config=None,
        pod_template_spec_config=None,
        pod_spec_config=None,
        job_config=None,
        job_spec_config=None,
    ):

        container_config = check.opt_str_param(container_config, 'container_config', {})
        pod_template_spec_config = check.opt_str_param(
            pod_template_spec_config, 'pod_template_spec_config', {}
        )
        pod_spec_config = check.opt_str_param(pod_spec_config, 'pod_spec_config', {})
        job_config = check.opt_str_param(job_config, 'job_config', {})
        job_spec_config = check.opt_str_param(job_spec_config, 'job_spec_config', {})

        # Additional checks
        if 'spec' in pod_template_spec_config:
            raise DagsterInvariantViolationError("...")

        if 'spec' in job_config:
            raise DagsterInvariantViolationError("...")

        return super(DagsterK8sConfig, cls).__new__(
            cls,
            container_config=check.opt_str_param(container_config, 'container_config'),
            pod_template_spec_config=check.opt_str_param(
                pod_template_spec_config, 'pod_template_spec_config'
            ),
            pod_spec_config=check.opt_str_param(pod_spec_config, 'pod_spec_config'),
            job_config=check.opt_str_param(job_config, 'job_config'),
            job_spec_config=check.opt_str_param(job_spec_config, 'job_spec_config'),
        )


def get_k8s_resource_requirements(tags):
    check.inst_param(tags, 'tags', frozentags)

    if not K8S_RESOURCE_REQUIREMENTS_KEY in tags:
        return None

    resource_requirements = json.loads(tags[K8S_RESOURCE_REQUIREMENTS_KEY])
    result = validate_config(K8S_RESOURCE_REQUIREMENTS_SCHEMA, resource_requirements)

    if not result.success:
        raise DagsterInvalidConfigError(
            'Error in tags for {}'.format(K8S_RESOURCE_REQUIREMENTS_KEY), result.errors, result,
        )

    return result.value


def get_k8s_config_from_tags(tags):
    check.inst_param(tags, 'tags', frozentags)

    if not K8S_CONFIG_KEY in tags:
        return DagsterK8sConfig()

    k8s_config = json.loads(tags[K8S_CONFIG_KEY])
    result = validate_config(K8S_CONFIG_SCHEMA, k8s_config)
    if not result.success:
        raise DagsterInvalidConfigError(
            'Error in tags for {}'.format(K8S_CONFIG_SCHEMA), result.errors, result,
        )

    k8s_config_value = result.value

    # Backcompat for resource requirements key
    resource_requirements_config = get_k8s_resource_requirements(tags)
    container_config = k8s_config_value.get('container_config', {})
    container_config = merge_dicts(container_config, {'resources': resource_requirements_config})

    return DagsterK8sConfig(
        container_config=container_config,
        pod_template_spec_config=k8s_config_value.get('pod_template_spec_config'),
        pod_spec_config=k8s_config_value.get('pod_spec_config'),
        job_config=k8s_config_value.get('job_config'),
        job_spec_config=k8s_config_value.get('job_spec_config'),
    )
