from dagster.core.definitions.reconstructable import ReconstructablePipeline
from dagster.core.errors import DagsterSingleProcessOnlyResourceError


def ensure_multiprocess_safe(pipeline, instance, mode_def, intermediate_storage_def):
    _check_intra_process_pipeline(pipeline)
    _check_non_ephemeral_instance(instance)
    _check_persistent_storage_requirement(
        pipeline.get_definition(), mode_def, intermediate_storage_def,
    )


def _check_intra_process_pipeline(pipeline):
    if not isinstance(pipeline, ReconstructablePipeline):
        raise DagsterSingleProcessOnlyResourceError(
            'You have attempted to use an executor that uses multiple processes with the pipeline "{name}" '
            "that is not reconstructable. Pipelines must be loaded in a way that allows dagster to reconstruct "
            "them in a new process. This means: \n"
            "  * using the file, module, or repository.yaml arguments of dagit/dagster-graphql/dagster\n"
            "  * loading the pipeline through the reconstructable() function\n".format(
                name=pipeline.get_definition().name
            )
        )


def _all_outputs_non_mem_asset_stores(pipeline_def, mode_def):
    """Returns true if every output definition in the pipeline uses an asset store that's not
    the mem_asset_store.

    If true, this indicates that it's OK to execute steps in their own processes, because their
    outputs will be available to other processes.
    """
    # pylint: disable=comparison-with-callable
    from dagster.core.storage.asset_store import mem_asset_store

    output_defs = [
        output_def
        for solid_def in pipeline_def.all_solid_defs
        for output_def in solid_def.output_defs
    ]
    for output_def in output_defs:
        if mode_def.resource_defs[output_def.asset_store_key] == mem_asset_store:
            return False

    return True


def _check_persistent_storage_requirement(pipeline_def, mode_def, intermediate_storage_def):
    """We prefer to store outputs with asset stores, but will fall back to intermediate storage
    if an asset store isn't set and will fall back to system storage if neither an asset
    store nor an intermediate storage are set.
    """
    if not (
        _all_outputs_non_mem_asset_stores(pipeline_def, mode_def)
        or (intermediate_storage_def and intermediate_storage_def.is_persistent)
    ):
        raise DagsterSingleProcessOnlyResourceError(
            "You have attempted to use an executor that uses multiple processes, but your pipeline "
            "includes solid outputs that will not be stored somewhere where other processes can"
            "retrieve them. "
            "Please make sure that your pipeline definition includes a ModeDefinition whose "
            'resource_keys assign the "asset_store" key to an AssetStore resource '
            "that stores outputs outside of the process, such as the fs_asset_store."
        )


def _check_non_ephemeral_instance(instance):
    if instance.is_ephemeral:
        raise DagsterSingleProcessOnlyResourceError(
            "You have attempted to use an executor that uses multiple processes with an "
            "ephemeral DagsterInstance. A non-ephemeral instance is needed to coordinate "
            "execution between multiple processes. You can configure your default instance "
            "via $DAGSTER_HOME or ensure a valid one is passed when invoking the python APIs."
        )
