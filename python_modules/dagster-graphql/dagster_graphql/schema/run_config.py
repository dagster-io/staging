import graphene
from dagster import check
from dagster.core.host_representation import RepresentedPipeline

from ..implementation.run_config_schema import resolve_is_run_config_valid
from .config_types import ConfigType, to_config_type
from .errors import ModeNotFoundError, PipelineNotFoundError, PythonError
from .pipelines.config_result import PipelineConfigValidationResult
from .runs import RunConfigData
from .util import non_null_list


class RunConfigSchema(graphene.ObjectType):
    def __init__(self, represented_pipeline, mode):
        super().__init__()
        self._represented_pipeline = check.inst_param(
            represented_pipeline, "represented_pipeline", RepresentedPipeline
        )
        self._mode = check.str_param(mode, "mode")

    class Meta:
        description = """The run config schema represents the all the config type
        information given a certain execution selection and mode of execution of that
        selection. All config interactions (e.g. checking config validity, fetching
        all config types, fetching in a particular config type) should be done
        through this type """

    rootConfigType = graphene.Field(
        graphene.NonNull(ConfigType),
        description="""Fetch the root environment type. Concretely this is the type that
        is in scope at the root of configuration document for a particular execution selection.
        It is the type that is in scope initially with a blank config editor.""",
    )
    allConfigTypes = graphene.Field(
        non_null_list(ConfigType),
        description="""Fetch all the named config types that are in the schema. Useful
        for things like a type browser UI, or for fetching all the types are in the
        scope of a document so that the index can be built for the autocompleting editor.
    """,
    )

    isRunConfigValid = graphene.Field(
        graphene.NonNull(PipelineConfigValidationResult),
        args={"runConfigData": graphene.Argument(RunConfigData)},
        description="""Parse a particular environment config result. The return value
        either indicates that the validation succeeded by returning
        `PipelineConfigValidationValid` or that there are configuration errors
        by returning `PipelineConfigValidationInvalid' which containers a list errors
        so that can be rendered for the user""",
    )

    def resolve_allConfigTypes(self, _graphene_info):
        return sorted(
            list(
                map(
                    lambda key: to_config_type(
                        self._represented_pipeline.config_schema_snapshot, key
                    ),
                    self._represented_pipeline.config_schema_snapshot.all_config_keys,
                )
            ),
            key=lambda ct: ct.key,
        )

    def resolve_rootConfigType(self, _graphene_info):
        return to_config_type(
            self._represented_pipeline.config_schema_snapshot,
            self._represented_pipeline.get_mode_def_snap(self._mode).root_config_key,
        )

    def resolve_isRunConfigValid(self, graphene_info, **kwargs):
        return resolve_is_run_config_valid(
            graphene_info, self._represented_pipeline, self._mode, kwargs.get("runConfigData", {}),
        )


class RunConfigSchemaOrError(graphene.Union):
    class Meta:
        from .pipelines.pipeline_errors import InvalidSubsetError

        types = (
            RunConfigSchema,
            PipelineNotFoundError,
            InvalidSubsetError,
            ModeNotFoundError,
            PythonError,
        )
