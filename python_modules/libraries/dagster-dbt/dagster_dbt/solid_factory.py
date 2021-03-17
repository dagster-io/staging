from typing import Any, Callable, Dict, Iterator, List, Optional

from dagster import (
    AssetMaterialization,
    EventMetadataEntry,
    InputDefinition,
    Nothing,
    Output,
    OutputDefinition,
    SolidExecutionContext,
    solid,
)

### CHANGES PART 1 ###

# With every invocation, dbt generates and saves one or more artifacts. These artifacts are in JSON.
# This type is used as Output for dbt solids and as a parameter type for the assets function.
# As of dbt v0.19.0, the following artifacts are available:
# - manifest.json
# - run_results.json
# - catalog.json
# - sources.json
DbtArtifact = Dict[str, Any]


# The user may want to yield assets differently for each "type" of dbt artifact. In this appraoch,
# when a dbt solid executes and fetches dbt artifacts, each artifact will be wrapped in one the
# following subclasses. This allows the user to later use `isinstance` if they wish to have
# specialized behavior for each dbt artifact.
class ManifestArtifact(DbtArtifact):
    pass


class RunResultsArtifact(DbtArtifact):
    pass


class CatalogArtifact(DbtArtifact):
    pass


class SourcesArtifact(DbtArtifact):
    pass


### CHANGES PART 2 ###

# This type OutputFn defines the function signature of the user-provided iterator.
# A user can define their own function with this signature to yield output.
# See some examples below.
OutputFn = Callable[
    [SolidExecutionContext, DbtArtifact],  # Argument List
    Iterator[Output],  # Return Type
]

# Examples


def yield_any_artifact_as_output(
    _context: SolidExecutionContext,
    artifact: DbtArtifact,
) -> Iterator[Output]:
    yield Output(value=artifact)


def yield_manifest_artifact_as_output(
    _context: SolidExecutionContext,
    artifact: DbtArtifact,
) -> Iterator[Output]:
    if isinstance(artifact, ManifestArtifact):
        yield Output(value=artifact)


def yield_no_outputs(*_args, **_kwargs):
    raise StopIteration


### CHANGES PART 3 ###

# This type AssetFn defines the function signature of the user-provided iterator.
# A user can define their own function with this signature to yield assets.
# See some examples below.
AssetFn = Callable[
    [SolidExecutionContext, DbtArtifact],  # Argument List
    Iterator[AssetMaterialization],  # Return Type
]

# Examples


def yield_any_artifact_as_asset(
    _context: SolidExecutionContext,
    artifact: DbtArtifact,
) -> Iterator[AssetMaterialization]:
    yield AssetMaterialization(
        asset_key="asset_key",
        metadata_entries=[
            EventMetadataEntry.json(data=artifact, label="label"),
        ],
    )


def yield_manifest_artifact_as_asset(
    _context: SolidExecutionContext,
    artifact: DbtArtifact,
) -> Iterator[AssetMaterialization]:
    if isinstance(artifact, ManifestArtifact):
        yield AssetMaterialization(
            asset_key="asset_key",
            metadata_entries=[
                EventMetadataEntry.json(data=artifact, label="label"),
            ],
        )


def yield_no_assets(*_args, **_kwargs):
    raise StopIteration


### CHANGES PART 4 ###

DEFAULT_INPUT_DEFS = [InputDefinition(name="start_after", dagster_type=Nothing)]
DEFAULT_OUTPUT_DEFS = None
DEFAULT_OUTPUT_FN = yield_no_outputs
DEFAULT_ASSET_FN = yield_no_assets


def create_dbt_solid(
    command: str,
    input_defs: Optional[
        List[InputDefinition]
    ] = DEFAULT_INPUT_DEFS,  # pylint: disable=dangerous-default-value
    output_defs: Optional[List[OutputDefinition]] = DEFAULT_OUTPUT_DEFS,
    output_fn: OutputFn = DEFAULT_OUTPUT_FN,
    asset_fn: AssetFn = DEFAULT_ASSET_FN,
    **solid_kwargs,
):
    @solid(
        input_defs=input_defs,
        output_defs=output_defs,
        **solid_kwargs,
    )
    def _solid(_context: SolidExecutionContext):
        # (Pseudo-code) Execute the dbt command.
        # Due to having 3 dbt workflows, this solid factory may need to be refactored into 3
        # separate solid factories.
        invoke_on_cli(command)  # pylint: disable=undefined-variable
        invoke_on_rpc(command)  # pylint: disable=undefined-variable
        invoke_on_cloud(command)  # pylint: disable=undefined-variable

        # (Pseudo-code) Fetch the dbt artifacts, or handle errors.
        # This logic will also depend on which of the 3 dbt workflows is being used.
        artifacts: List[DbtArtifact] = fetch_artifacts()  # pylint: disable=undefined-variable

        # (Working code) Apply OutputFn and AssetFn to artifacts.
        for output_iter in map(output_fn, artifacts):
            for output in output_iter:
                yield output

        for asset_iter in map(asset_fn, artifacts):
            for asset in asset_iter:
                yield asset

    return _solid
