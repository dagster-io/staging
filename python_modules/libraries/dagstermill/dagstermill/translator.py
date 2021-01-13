from typing import Any, Dict

import papermill
from dagster import check, seven

RESERVED_INPUT_NAMES = [
    "__dagstermill_context",
    "__dagstermill_dagstermill",
    "__dagstermill_executable_dict",
    "__dagstermill_json",
    "__dagstermill_pipeline_run_dict",
    "__dagstermill_solid_handle_kwargs",
    "__dagstermill_instance_ref_dict",
    "__dagstermill_seven",
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
    }}
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

        content = INJECTED_BOILERPLATE.format(pipeline_context_args=pipeline_context_args)

        for name, val in parameters.items():
            dm_unmarshal_call = (
                f"__dagstermill_dagstermill._load_parameter('{name}', '{seven.json.dumps(val)}')"
            )
            content += "{}\n".format(cls.assign(name, dm_unmarshal_call))

        return content


papermill.translators.papermill_translators.register("python", DagsterTranslator)
