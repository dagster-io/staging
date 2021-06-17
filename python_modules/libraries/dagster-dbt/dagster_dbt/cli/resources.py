from typing import Any, Dict, List, Optional

from dagster import Field, Permissive, check, resource
from dagster.utils.merger import deep_merge_dicts

from ..dbt_resource import DbtResource
from .constants import CLI_COMMON_FLAGS_CONFIG_SCHEMA, CLI_COMMON_OPTIONS_CONFIG_SCHEMA
from .types import DbtCliOutput
from .utils import execute_cli


class DbtCliResource(DbtResource):
    """
    A resource that allows you to execute dbt cli commands. For the most up-to-date documentation on
    the specific parameters available to you for each command, check out the dbt docs:

    https://docs.getdbt.com/reference/commands/run
    """

    def __init__(
        self,
        executable: str,
        default_flags: Dict[str, Any],
        warn_error: bool,
        ignore_handled_error: bool,
        target_path: str,
        logger: Optional[Any] = None,
    ):
        self._default_flags = default_flags
        self._executable = executable
        self._warn_error = warn_error
        self._ignore_handled_error = ignore_handled_error
        self._target_path = target_path
        super().__init__(logger)

    @property
    def default_flags(self) -> Dict[str, Any]:
        # return a non-frozendict copy of self._default_flags

        def unfreeze(d):
            if isinstance(d, dict):
                return {k: unfreeze(v) for k, v in d.items()}
            return d

        return unfreeze(self._default_flags)

    def cli(self, command: str, **kwargs) -> DbtCliOutput:
        """
        Executes a dbt CLI command. Params passed in as keyword arguments will be merged with the
            default flags that were configured on resource initialization (if any) overriding the
            default values if necessary.

        Args:
            command (str): The command you wish to run (e.g. 'run', 'test', 'docs generate', etc.)

        Returns:
            DbtCliOutput: An instance of :class:`DbtCliOutput<dagster_dbt.DbtCliOutput>` containing
                parsed log output as well as the contents of run_results.json (if applicable).
        """
        command = check.str_param(command, "command")
        extra_flags = {} if kwargs is None else kwargs
        flags = self._format_params(deep_merge_dicts(self.default_flags, extra_flags))

        return execute_cli(
            executable=self._executable,
            command=command,
            flags_dict=flags,
            log=self.logger,
            warn_error=self._warn_error,
            ignore_handled_error=self._ignore_handled_error,
            target_path=self._target_path,
        )

    def compile(self, models: List[str] = None, exclude: List[str] = None, **kwargs):
        """
        Run the ``compile`` command on a dbt project. kwargs are passed in as additional parameters.

        Args:
            models (List[str], optional): the models to include in compilation.
            exclude (List[str]), optional): the models to exclude from compilation.
        """
        return self.cli("compile", models=models, exclude=exclude, **kwargs)

    def run(self, models: List[str] = None, exclude: List[str] = None, **kwargs):
        """
        Run the ``run`` command on a dbt project. kwargs are passed in as additional parameters.

        Args:
            models (List[str], optional): the models to include in compilation.
            exclude (List[str]), optional): the models to exclude from compilation.
        """
        return self.cli("run", models=models, exclude=exclude, **kwargs)

    def snapshot(self, select: List[str] = None, exclude: List[str] = None, **kwargs):
        """
        Run the ``snapshot`` command on a dbt project. kwargs are passed in as additional parameters.

        Args:
            select (List[str], optional): the snapshots to include in the run.
            exclude (List[str], optional): the snapshots to exclude from the run.
        """
        return self.cli("snapshot", select=select, exclude=exclude, **kwargs)

    def test(
        self,
        models: List[str] = None,
        exclude: List[str] = None,
        data: bool = True,
        schema: bool = True,
        **kwargs,
    ):
        """
        Run the ``test`` command on a dbt project. kwargs are passed in as additional parameters.

        Args:
            models (List[str], optional): the models to include in testing.
            exclude (List[str], optional): the models to exclude from testing.
            data (bool, optional): If ``True`` (default), then run data tests.
            schema (bool, optional): If ``True`` (default), then run schema tests.

        """
        return self.cli("test", models=models, exclude=exclude, data=data, schema=schema, **kwargs)

    def seed(self, show: bool = False, **kwargs):
        """
        Run the ``seed`` command on a dbt project. kwargs are passed in as additional parameters.

        Args:
            show (bool, optional): If ``True``, then show a sample of the seeded data in the
                response. Defaults to ``False``.

        """
        return self.cli("seed", show=show, **kwargs)

    def generate_docs(self, models: List[str] = None, exclude: List[str] = None, **kwargs):
        """
        Run the ``docs generate`` command on a dbt project. kwargs are passed in as additional parameters.

        Args:
            models (List[str], optional): the models to include in docs generation.
            exclude (List[str], optional): the models to exclude from docs generation.

        """
        return self.cli("docs generate", models=models, exclude=exclude, **kwargs)

    def run_operation(self, macro: str, args: Optional[Dict[str, Any]] = None, **kwargs):
        """
        Run the ``run-operation`` command on a dbt project. kwargs are passed in as additional parameters.

        Args:
            macro (str): the dbt macro to invoke.
            args (Dict[str, Any], optional): the keyword arguments to be supplied to the macro.

        Returns:
            Response: the HTTP response from the dbt RPC server.
        """

        return self.cli(f"run-operation {macro}", args=args, **kwargs)


@resource(
    config_schema={
        "default_flags": Field(
            config=Permissive(CLI_COMMON_FLAGS_CONFIG_SCHEMA),
            is_required=False,
            description="The default set of flags that will be passed in to every CLI invocation.",
        ),
        **CLI_COMMON_OPTIONS_CONFIG_SCHEMA,
    },
    description="A resource that can run dbt CLI commands.",
)
def dbt_cli_resource(context):
    return DbtCliResource(
        executable=context.resource_config["dbt_executable"],
        default_flags=context.resource_config["default_flags"],
        warn_error=context.resource_config["warn-error"],
        ignore_handled_error=context.resource_config["ignore_handled_error"],
        target_path=context.resource_config["target-path"],
        logger=context.log,
    )
