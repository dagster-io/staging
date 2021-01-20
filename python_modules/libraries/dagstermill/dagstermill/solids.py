import copy
import os
import pickle
import sys
import tempfile
import uuid
from typing import Any, Dict, List, Optional, Tuple

import nbformat
import papermill
from dagster import (
    AssetMaterialization,
    EventMetadataEntry,
    InputDefinition,
    Output,
    OutputDefinition,
    SolidDefinition,
    check,
    seven,
)
from dagster.core.definitions.reconstructable import ReconstructablePipeline
from dagster.core.execution.context.compute import SolidExecutionContext
from dagster.core.execution.context.system import SystemStepExecutionContext
from dagster.core.execution.plan.inputs import FromStepOutput
from dagster.core.storage.file_manager import FileHandle, FileManager
from dagster.core.storage.mem_io_manager import InMemoryIOManager
from dagster.serdes import pack_value
from dagster.utils.error import serializable_error_info_from_exc_info
from papermill.engines import papermill_engines
from papermill.iorw import load_notebook_node, write_ipynb
from papermill.parameterize import _find_first_tagged_cell_index

from .engine import DagstermillNBConvertEngine
from .errors import DagstermillError
from .io import read_notebook_output
from .translator import DagsterTranslator


# This is based on papermill.parameterize.parameterize_notebook
# Typically, papermill injects the injected-parameters cell *below* the parameters cell
# but we want to *replace* the parameters cell, which is what this function does.
def replace_parameters(context, nb, parameters):
    """Assigned parameters into the appropiate place in the input notebook

    Args:
        nb (NotebookNode): Executable notebook object
        parameters (dict): Arbitrary keyword arguments to pass to the notebook parameters.
    """
    check.dict_param(parameters, "parameters")

    # Copy the nb object to avoid polluting the input
    nb = copy.deepcopy(nb)

    # papermill method chooses translator based on kernel_name and language, but we just call the
    # DagsterTranslator to generate parameter content based on the kernel_name
    param_content = DagsterTranslator.codify(parameters)

    newcell = nbformat.v4.new_code_cell(source=param_content)
    newcell.metadata["tags"] = ["injected-parameters"]

    param_cell_index = _find_first_tagged_cell_index(nb, "parameters")
    injected_cell_index = _find_first_tagged_cell_index(nb, "injected-parameters")
    if injected_cell_index >= 0:
        # Replace the injected cell with a new version
        before = nb.cells[:injected_cell_index]
        after = nb.cells[injected_cell_index + 1 :]
        check.int_value_param(param_cell_index, -1, "param_cell_index")
        # We should have blown away the parameters cell if there is an injected-parameters cell
    elif param_cell_index >= 0:
        # Replace the parameter cell with the injected-parameters cell
        before = nb.cells[:param_cell_index]
        after = nb.cells[param_cell_index + 1 :]
    else:
        # Inject to the top of the notebook, presumably first cell includes dagstermill import
        context.log.debug(
            (
                "Executing notebook with no tagged parameters cell: injecting boilerplate in first "
                "cell."
            )
        )
        before = []
        after = nb.cells

    nb.cells = before + [newcell] + after
    nb.metadata.papermill["parameters"] = seven.json.dumps(parameters)

    return nb


def get_papermill_parameters(
    context: SystemStepExecutionContext,
    inputs: Dict[str, Any],
    output_log_path: str,
    marshal_dir: str,
):
    check.inst_param(context, "context", SystemStepExecutionContext)
    check.param_invariant(
        isinstance(context.run_config, dict),
        "context",
        "context passed to get_papermill_parameters must have valid run_config",
    )
    check.dict_param(inputs, "inputs", key_type=str)
    check.str_param(output_log_path, "output_log_path")
    check.str_param(marshal_dir, "marshal_dir")

    if not isinstance(context.pipeline, ReconstructablePipeline):
        raise DagstermillError(
            "Can't execute a Dagstermill solid from a pipeline that is not reconstructable. "
            "Use the reconstructable() function if executing from python. See the Dagstermill "
            "documentation for details."
        )

    dm_executable_dict = context.pipeline.to_dict()

    dm_context_dict = {
        "output_log_path": output_log_path,
        "marshal_dir": marshal_dir,
        "run_config": context.run_config,
    }

    dm_solid_handle_kwargs = context.solid_handle._asdict()

    parameters: Dict[str, Any] = {}

    parameters["context_dict"] = dm_context_dict
    parameters["executable_dict"] = dm_executable_dict
    parameters["pipeline_run_dict"] = pack_value(context.pipeline_run)
    parameters["solid_handle_kwargs"] = dm_solid_handle_kwargs
    parameters["instance_ref_dict"] = pack_value(context.instance.get_ref())
    parameters["input_names"] = list(inputs.keys())
    parameters["step_key"] = context.step.key

    return parameters


def _events_for_output_notebook(
    output_notebook_path: str,
    solid_context: SolidExecutionContext,
    step_context: SystemStepExecutionContext,
):
    """Helper function to generate the event stream corresponding to a completed notebook execution.

    We use scrapbook to pass events back from the Dagstermill notebook kernel; this lets us yield
    events from the solid compute function rather than having to invoke the event-handling code on
    the other side of the process boundary.

    Outputs get special handling because we are using the IOManager machinery to pass them across
    the process boundary.
    """
    # Deferred import for performance
    import scrapbook

    output_nb = scrapbook.read_notebook(output_notebook_path)

    # We use dummy events starting with the special prefix "output-" to indicate which
    # outputs the notebook actually yielded -- this lets us handle optional outputs
    # correctly. All other events start with the prefix "event-".

    outputs = []
    events = []

    for key, value in output_nb.scraps.items():
        if key.startswith("output-"):
            outputs.append(value)
        elif key.startswith("event-"):
            events.append((key, value))

    # FIXME: Maybe figure out how to preserve event order out of the notebook
    # Deal with outputs first. We read in the dummy output events to see which outputs were
    # actually yielded.
    outputs_to_handle = set([])
    for output in outputs:
        with open(output.data, "rb") as fd:
            dummy_output_event = pickle.loads(fd.read())
        outputs_to_handle.add(dummy_output_event.output_name)

    for (output_name, _output_def) in step_context.solid_def.output_dict.items():
        if output_name not in outputs_to_handle:
            continue

        obj = read_notebook_output(
            solid_context=solid_context, step_context=step_context, output_name=output_name,
        )

        yield Output(obj, output_name)

    for key, value in output_nb.scraps.items():
        if key.startswith("event-"):
            with open(value.data, "rb") as fd:
                yield pickle.loads(fd.read())

    return
    yield  # pylint: disable=unreachable


def _dm_solid_compute(
    name: str, notebook_path: str, output_notebook: str = None,
):
    """Factory function for Dagstermill solid compute functions."""
    check.str_param(name, "name")
    check.str_param(notebook_path, "notebook_path")
    check.opt_str_param(output_notebook, "output_notebook")

    def _t_fn(context: SolidExecutionContext, inputs):
        check.inst_param(context, "context", SolidExecutionContext)
        check.param_invariant(
            isinstance(context.run_config, dict),
            "context",
            "Execution context for Dagstermill solid must have valid run_config",
        )

        step_context = context.get_system_context()

        output_defs = step_context.solid_def.output_defs

        for output_def in output_defs:
            output_name = output_def.name
            io_manager_key = output_def.io_manager_key

            io_manager = getattr(step_context.resources, io_manager_key)

            # Dagstermill requires IOManagers that can pass values across process boundaries; there's
            # no good way to check for this in general, so we check that we aren't using the default
            # in-memory IO manager and throw an informative error in this case.
            if isinstance(io_manager, InMemoryIOManager):
                if io_manager_key == "io_manager":
                    error_message = (
                        "May not use the default mem_io_manager when executing Dagstermill solids "
                        f"(default io_manager_key {io_manager_key} for output {output_name}). "
                        "Please configure an IO manager resource that is appropriate for passing "
                        "values across process boundaries on this pipeline, e.g., "
                        "dagster.fs_io_manager. See the Dagstermill documentation for details."
                    )
                else:
                    error_message = (
                        "May not use the mem_io_manager when executing Dagstermill solids "
                        f"(io_manager_key {io_manager_key} for output {output_name}). "
                        "Please configure an IO manager resource that is appropriate for passing "
                        "values across process boundaries on this output, e.g., "
                        "dagster.fs_io_manager. See the Dagstermill documentation for details."
                    )
                raise DagstermillError(error_message)

        step_inputs = step_context.step.step_inputs

        for step_input in step_inputs:
            if isinstance(step_input.source, FromStepOutput):
                source_handle = step_input.source.step_output_handle
                input_manager = step_context.get_output_manager(source_handle)
                io_manager_key = step_context.execution_plan.get_manager_key(source_handle)
                if isinstance(input_manager, InMemoryIOManager):
                    source_output = source_handle.output_name
                    source_step_key = source_handle.step_key
                    if io_manager_key == "io_manager":
                        raise DagstermillError(
                            "May not use the default mem_io_manager when executing Dagstermill "
                            f"solids (default io_manager_key {io_manager_key} for input "
                            f"{step_input.name} with source output {source_output} from step "
                            f"{source_step_key}). Please configure an IO manager resource that is "
                            "appropriate for passing values across process boundaries on your "
                            "pipeline, e.g., dagster.fs_io_manager. See the Dagstermill "
                            "documentation for details."
                        )
                    else:
                        raise DagstermillError(
                            "May not use the mem_io_manager when executing Dagstermill "
                            f"solids (io_manager_key {io_manager_key} for input {step_input.name} "
                            f"with source output {source_output} from step {source_step_key}). "
                            "Please configure an IO manager resource that is appropriate for "
                            "passing values across process boundaries on this output, e.g., "
                            "dagster.fs_io_manager. See the Dagstermill documentation for details."
                        )

        with tempfile.TemporaryDirectory() as temp_dir:
            # Papermill operates on local files, so we need local paths for the input and output
            # notebooks.
            nb_uuid = str(uuid.uuid4())
            output_log_path = os.path.join(temp_dir, f"{nb_uuid}-out.log")
            input_notebook_path = os.path.join(temp_dir, f"{nb_uuid}-in.ipynb")
            output_notebook_path = os.path.join(temp_dir, f"{nb_uuid}-out.ipynb")

            # Inject the runtime parameters cell into the input notebook.
            input_nb = load_notebook_node(notebook_path)
            unparametrized_nb = replace_parameters(
                step_context,
                input_nb,
                get_papermill_parameters(
                    step_context, inputs, output_log_path, marshal_dir=temp_dir
                ),
            )
            write_ipynb(unparametrized_nb, input_notebook_path)

            papermill_engines.register("dagstermill", DagstermillNBConvertEngine)

            try:
                papermill.execute_notebook(
                    input_path=input_notebook_path,
                    output_path=output_notebook_path,
                    engine_name="dagstermill",
                    log_output=True,
                )
            # If we encounter an exception during execution, we still want to return the executed
            # notebook if it's configured
            except Exception:  # pylint: disable=broad-except
                if output_notebook is not None:
                    try:
                        with open(output_notebook_path, "rb") as fd:
                            yield Output(fd.read(), output_name=output_notebook)
                    except Exception:  # pylint: disable=broad-except
                        pass
                raise

            if output_notebook is not None:
                with open(output_notebook_path, "rb") as fd:
                    yield Output(fd.read(), output_name=output_notebook)

            yield from _events_for_output_notebook(
                output_notebook_path=output_notebook_path,
                solid_context=context,
                step_context=step_context,
            )

    return _t_fn


def dagstermill_solid(
    name,
    notebook_path,
    input_defs=None,
    output_defs=None,
    config_schema=None,
    required_resource_keys=None,
    output_notebook=None,
    output_notebook_io_manager_key="io_manager",
):
    """Wrap a Jupyter notebook in a solid.

    Arguments:
        name (str): The name of the solid.
        notebook_path (str): Path to the backing notebook.
        input_defs (Optional[List[InputDefinition]]): The solid's inputs.
        output_defs (Optional[List[OutputDefinition]]): The solid's outputs. Your notebook should
            call :py:func:`~dagstermill.yield_result` to yield each of these outputs.
        required_resource_keys (Optional[Set[str]]): The string names of any required resources.
        output_notebook (Optional[str]): If set, will be used as the name of an injected output of
            type :py:class:`python:bytes` that will contain the notebook as executed. Set this if
            you want the executed notebook to be available to downstream computation.
        output_notebook_io_manager_key (Optional[str]): If set, will be used as the io manager key
            for the injected executed notebook output. Default: `"io_manager"`.

    Examples:
        FIXME: Need examples

    Returns:
        :py:class:`~dagster.SolidDefinition`
    """
    check.str_param(name, "name")
    check.str_param(notebook_path, "notebook_path")
    input_defs = check.opt_list_param(input_defs, "input_defs", of_type=InputDefinition)
    output_defs_list = check.opt_list_param(output_defs, "output_defs", of_type=OutputDefinition)
    required_resource_keys = check.opt_set_param(
        required_resource_keys, "required_resource_keys", of_type=str
    )
    output_notebook = check.opt_str_param(output_notebook, "output_notebook")

    check.invariant(
        output_notebook not in [output_def.name for output_def in output_defs_list],
        (
            f"Can't emit output notebook with output name '{output_notebook}': there is another "
            "output defined with this name."
        ),
    )

    required_resource_keys.update(
        {output_def.io_manager_key or "io_manager" for output_def in output_defs_list}
    )

    output_defs_list = output_defs_list + (
        [OutputDefinition(name=output_notebook, io_manager_key=output_notebook_io_manager_key)]
        if output_notebook
        else []
    )

    compute_fn = _dm_solid_compute(
        name=name, notebook_path=notebook_path, output_notebook=output_notebook,
    )

    return SolidDefinition(
        name=name,
        input_defs=input_defs,
        compute_fn=compute_fn,
        output_defs=output_defs_list,
        config_schema=config_schema,
        required_resource_keys=required_resource_keys,
        description="This solid is backed by the notebook at {path}".format(path=notebook_path),
        tags={"notebook_path": notebook_path, "kind": "ipynb"},
    )
