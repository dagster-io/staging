from typing import Any, Dict

import papermill
from dagster import check, seven

LEGACY_RESERVED_INPUT_NAMES = [
    "__dm_context",
    "__dagstermill_dagstermill",
    "__dagstermill_seven",
    "__dm_executable_dict",
    "__dm_json",
    "__dm_pipeline_run_dict",
    "__dm_solid_handle_kwargs",
    "__dm_instance_ref_dict",
    "__dm_step_key",
]

INJECTED_BOILERPLATE = """
# Injected parameters
from dagster import seven as __dagstermill_seven
import dagstermill as __dagstermill_dagstermill
context = __dagstermill_dagstermill._reconstitute_pipeline_context(
    **{{
        key: __dagstermill_seven.json.loads(value)
        for key, value
        in {pipeline_context_args}.items()
    }},
    step_key="{step_key}",
    legacy={legacy_flag},
)
"""


class DagsterTranslator(papermill.translators.PythonTranslator):
    @classmethod
    def codify(cls, parameters: Dict[str, Any]):
        check.dict_elem(parameters, "context_dict")
        check.dict_elem(parameters, "executable_dict")
        check.dict_elem(parameters, "pipeline_run_dict")
        check.dict_elem(parameters, "solid_handle_kwargs")
        check.dict_elem(parameters, "instance_ref_dict")
        check.list_elem(parameters, "input_names")
        check.str_elem(parameters, "step_key")

        context_args = parameters["context_dict"]
        pipeline_context_args = dict(
            executable_dict=parameters["executable_dict"],
            pipeline_run_dict=parameters["pipeline_run_dict"],
            solid_handle_kwargs=parameters["solid_handle_kwargs"],
            instance_ref_dict=parameters["instance_ref_dict"],
            **context_args,
        )

        for key in pipeline_context_args:
            pipeline_context_args[key] = seven.json.dumps(pipeline_context_args[key])

        content = INJECTED_BOILERPLATE.format(
            pipeline_context_args=pipeline_context_args,
            step_key=parameters["step_key"],
            legacy_flag="False",
        )

        for input_name in parameters["input_names"]:
            dm_unmarshal_call = f"__dagstermill_dagstermill._load_input('{input_name}')"
            content += "{}\n".format(cls.assign(input_name, dm_unmarshal_call))

        return content

    @classmethod
    def legacy_codify(cls, parameters: Dict[str, Any]):
        """Deprecated and will be removed when the legacy define_dagstermill_solid API is removed
        in 0.11.0."""
        assert "__dm_context" in parameters
        assert "__dm_executable_dict" in parameters
        assert "__dm_pipeline_run_dict" in parameters
        assert "__dm_solid_handle_kwargs" in parameters
        assert "__dm_instance_ref_dict" in parameters
        assert "__dm_step_key" in parameters

        context_args = parameters["__dm_context"]
        pipeline_context_args = dict(
            executable_dict=parameters["__dm_executable_dict"],
            pipeline_run_dict=parameters["__dm_pipeline_run_dict"],
            solid_handle_kwargs=parameters["__dm_solid_handle_kwargs"],
            instance_ref_dict=parameters["__dm_instance_ref_dict"],
            **context_args,
        )

        for key in pipeline_context_args:
            pipeline_context_args[key] = seven.json.dumps(pipeline_context_args[key])

        content = INJECTED_BOILERPLATE.format(
            pipeline_context_args=pipeline_context_args,
            step_key=parameters["__dm_step_key"],
            legacy_flag="True",
        )

        for name, val in parameters.items():
            if name in LEGACY_RESERVED_INPUT_NAMES:
                continue
            dm_unmarshal_call = "__dagstermill_dagstermill._load_parameter('{name}', '{val}')".format(
                name=name, val=seven.json.dumps(val)
            )
            content += "{}\n".format(cls.assign(name, dm_unmarshal_call))

        return content
