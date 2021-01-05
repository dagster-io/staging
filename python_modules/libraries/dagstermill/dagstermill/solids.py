import copy
import os
import pickle
import sys
import tempfile
import uuid

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
from dagster.core.execution.context.system import (
    InputContext,
    OutputContext,
    SystemComputeExecutionContext,
)
from dagster.core.storage.asset_store import InMemoryAssetStore
from dagster.core.storage.file_manager import FileHandle
from dagster.core.storage.intermediate_storage import ObjectStoreIntermediateStorage
from dagster.serdes import pack_value
from dagster.utils import mkdir_p, safe_tempfile_path
from dagster.utils.error import serializable_error_info_from_exc_info
from papermill.engines import papermill_engines
from papermill.iorw import load_notebook_node, write_ipynb
from papermill.parameterize import _find_first_tagged_cell_index

from .engine import DagstermillNBConvertEngine
from .errors import DagstermillError
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


def get_papermill_parameters(compute_context, inputs, output_log_path, marshal_dir):
    check.inst_param(compute_context, "compute_context", SystemComputeExecutionContext)
    check.param_invariant(
        isinstance(compute_context.run_config, dict),
        "compute_context",
        "SystemComputeExecutionContext must have valid run_config",
    )
    check.dict_param(inputs, "inputs", key_type=str)

    run_id = compute_context.run_id

    if not isinstance(compute_context.pipeline, ReconstructablePipeline):
        raise DagstermillError(
            "Can't execute a dagstermill solid from a pipeline that is not reconstructable. "
            "Use the reconstructable() function if executing from python"
        )

    dm_executable_dict = compute_context.pipeline.to_dict()

    dm_context_dict = {
        "output_log_path": output_log_path,
        "marshal_dir": marshal_dir,
        "run_config": compute_context.run_config,
    }

    dm_solid_handle_kwargs = compute_context.solid_handle._asdict()

    parameters = {}

    parameters["context_dict"] = dm_context_dict
    parameters["executable_dict"] = dm_executable_dict
    parameters["pipeline_run_dict"] = pack_value(compute_context.pipeline_run)
    parameters["solid_handle_kwargs"] = dm_solid_handle_kwargs
    parameters["instance_ref_dict"] = pack_value(compute_context.instance.get_ref())
    parameters["step_key"] = compute_context.step.key
    parameters["input_names"] = list(inputs.keys())

    return parameters


def _dm_solid_compute(name, notebook_path, output_notebook=None, asset_key_prefix=None):
    check.str_param(name, "name")
    check.str_param(notebook_path, "notebook_path")
    check.opt_str_param(output_notebook, "output_notebook")
    check.opt_list_param(asset_key_prefix, "asset_key_prefix")

    def _t_fn(compute_context, inputs):
        check.inst_param(compute_context, "compute_context", SolidExecutionContext)
        check.param_invariant(
            isinstance(compute_context.run_config, dict),
            "context",
            "SystemComputeExecutionContext must have valid run_config",
        )

        system_compute_context = compute_context.get_system_context()

        object_manager = system_compute_context.resources.object_manager

        # FIXME: Is this really the blessed way to check whether the object manager is the in-memory
        # object manager
        if isinstance(object_manager, InMemoryAssetStore):
            raise DagstermillError(
                "May not use the default mem_asset_store when executing Dagstermill solids. "
                "Please configure an object manager resource that is appropriate for passing "
                "values across process boundaries on your pipeline, e.g., fs_asset_store."
            )

        with tempfile.TemporaryDirectory() as temp_dir:
            with safe_tempfile_path() as output_log_path:

                parameterized_notebook_path = os.path.join(
                    temp_dir, "{prefix}-inter.ipynb".format(prefix=str(uuid.uuid4()))
                )

                executed_notebook_path = os.path.join(
                    temp_dir, "{prefix}-out.ipynb".format(prefix=str(uuid.uuid4()))
                )

                # Scaffold the registration here
                nb = load_notebook_node(notebook_path)
                nb_no_parameters = replace_parameters(
                    system_compute_context,
                    nb,
                    get_papermill_parameters(
                        system_compute_context, inputs, output_log_path, marshal_dir=temp_dir
                    ),
                )
                write_ipynb(nb_no_parameters, parameterized_notebook_path)

                try:
                    papermill_engines.register("dagstermill", DagstermillNBConvertEngine)
                    papermill.execute_notebook(
                        input_path=parameterized_notebook_path,
                        output_path=executed_notebook_path,
                        engine_name="dagstermill",
                        log_output=True,
                    )

                except Exception:  # pylint: disable=broad-except
                    try:
                        with open(executed_notebook_path, "rb") as fd:
                            executed_notebook_file_handle = compute_context.resources.file_manager.write(
                                fd, mode="wb", ext="ipynb"
                            )
                            executed_notebook_materialization_path = (
                                executed_notebook_file_handle.path_desc
                            )
                    except Exception:  # pylint: disable=broad-except
                        compute_context.log.warning(
                            "Error when attempting to materialize executed notebook using file manager (falling back to local): {exc}".format(
                                exc=str(serializable_error_info_from_exc_info(sys.exc_info()))
                            )
                        )
                        executed_notebook_materialization_path = executed_notebook_path

                    yield AssetMaterialization(
                        asset_key=(asset_key_prefix + [f"{name}_output_notebook"]),
                        description="Location of output notebook in file manager",
                        metadata_entries=[
                            EventMetadataEntry.fspath(
                                executed_notebook_materialization_path,
                                label="executed_notebook_path",
                            )
                        ],
                    )
                    raise

            system_compute_context.log.debug(
                "Notebook execution complete for {name} at {executed_notebook_path}.".format(
                    name=name, executed_notebook_path=executed_notebook_path,
                )
            )

            executed_notebook_file_handle = None
            try:
                # use binary mode when copying the file contents since certain file_managers such
                # as S3 may try to hash the contents
                with open(executed_notebook_path, "rb") as fd:
                    executed_notebook_file_handle = compute_context.resources.file_manager.write(
                        fd, mode="wb", ext="ipynb"
                    )
                    executed_notebook_materialization_path = executed_notebook_file_handle.path_desc
            except Exception:  # pylint: disable=broad-except
                compute_context.log.warning(
                    "Error when attempting to materialize executed notebook using file manager "
                    "(falling back to local): "
                    f"{str(serializable_error_info_from_exc_info(sys.exc_info()))}"
                )
                executed_notebook_materialization_path = executed_notebook_path

            yield AssetMaterialization(
                asset_key=(asset_key_prefix + [f"{name}_output_notebook"]),
                description="Location of output notebook in file manager",
                metadata_entries=[
                    EventMetadataEntry.fspath(executed_notebook_materialization_path)
                ],
            )

            if output_notebook is not None:
                yield Output(executed_notebook_file_handle, output_notebook)

            # deferred import for perf
            import scrapbook

            output_nb = scrapbook.read_notebook(executed_notebook_path)

            # Optional output handling
            outputs_to_handle = set([])
            for key, value in output_nb.scraps.items():
                if key.startswith("output-"):
                    with open(value.data, "rb") as fd:
                        output_event = pickle.loads(fd.read())
                    outputs_to_handle.add(output_event.output_name)

            for (output_name, output_def) in system_compute_context.solid_def.output_dict.items():
                if output_name not in outputs_to_handle:
                    continue
                input_context = InputContext(
                    name=None,
                    pipeline_name=compute_context.pipeline_run.pipeline_name,
                    solid_def=None,
                    config=None,
                    metadata=None,
                    upstream_output=OutputContext(
                        step_key=system_compute_context.step.key,
                        name=output_name,
                        pipeline_name=compute_context.pipeline_run.pipeline_name,
                        run_id=system_compute_context.run_id,
                        metadata=None,
                        mapping_key=None,
                        config=None,
                        solid_def=compute_context.solid_def,
                        dagster_type=output_def.dagster_type,
                        log_manager=compute_context.log,
                        version=None,
                        step_context=None,  # FIXME: ?
                    ),
                    dagster_type=output_def.dagster_type,
                    log_manager=compute_context.log,
                    step_context=None,  # FIXME: ?
                    # step_key=system_compute_context.step.key,
                    # output_name=output_name,
                    # mapping_key=None,
                    # asset_metadata={},
                    # pipeline_name=compute_context.pipeline_run.pipeline_name,
                    # solid_def=compute_context.solid_def,
                    # dagster_type=output_def.dagster_type,
                    # # Consider postfixing this with _dagstermill
                    # source_run_id=system_compute_context.run_id,
                    # version=None,
                )
                obj = object_manager.load_input(context=input_context)

                yield Output(obj, output_name)

            # FIXME make sure this is a tempfile
            for key, value in output_nb.scraps.items():
                if key.startswith("event-"):
                    with open(value.data, "rb") as fd:
                        yield pickle.loads(fd.read())

    return _t_fn


def define_dagstermill_solid(
    name,
    notebook_path,
    input_defs=None,
    output_defs=None,
    config_schema=None,
    required_resource_keys=None,
    output_notebook=None,
    asset_key_prefix=None,
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
            type :py:class:`~dagster.FileHandle` that will point to the executed notebook (in
            addition to the :py:class:`~dagster.AssetMaterialization` that is always created). This
            respects the :py:class:`~dagster.core.storage.file_manager.FileManager` configured on
            the pipeline resources via the "file_manager" resource key, so, e.g.,
            if :py:class:`~dagster_aws.s3.s3_file_manager` is configured, the output will be a :
            py:class:`~dagster_aws.s3.S3FileHandle`.
        asset_key_prefix (Optional[Union[List[str], str]]): If set, will be used to prefix the
            asset keys for materialized notebooks.

    Returns:
        :py:class:`~dagster.SolidDefinition`
    """
    check.str_param(name, "name")
    check.str_param(notebook_path, "notebook_path")
    input_defs = check.opt_list_param(input_defs, "input_defs", of_type=InputDefinition)
    output_defs = check.opt_list_param(output_defs, "output_defs", of_type=OutputDefinition)
    required_resource_keys = check.opt_set_param(
        required_resource_keys, "required_resource_keys", of_type=str
    )
    if output_notebook is not None:
        required_resource_keys.add("file_manager")
    if isinstance(asset_key_prefix, str):
        asset_key_prefix = [asset_key_prefix]

    asset_key_prefix = check.opt_list_param(asset_key_prefix, "asset_key_prefix", of_type=str)

    return SolidDefinition(
        name=name,
        input_defs=input_defs,
        compute_fn=_dm_solid_compute(
            name, notebook_path, output_notebook, asset_key_prefix=asset_key_prefix
        ),
        output_defs=output_defs
        + (
            [OutputDefinition(dagster_type=FileHandle, name=output_notebook)]
            if output_notebook
            else []
        ),
        config_schema=config_schema,
        required_resource_keys=required_resource_keys,
        description="This solid is backed by the notebook at {path}".format(path=notebook_path),
        tags={"notebook_path": notebook_path, "kind": "ipynb"},
    )
