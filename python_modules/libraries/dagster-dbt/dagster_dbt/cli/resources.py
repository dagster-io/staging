from typing import Any, Dict

from dagster import Field, Permissive, check, resource
from dagster.utils.merger import deep_merge_dicts

from .constants import CLI_COMMON_FLAGS_CONFIG_SCHEMA, CLI_COMMON_OPTIONS_CONFIG_SCHEMA
from .utils import execute_cli, parse_run_results


class DbtCliResource:
    def __init__(
        self,
        executable: str,
        default_flags: Dict[str, Any],
        warn_error: bool,
        ignore_handled_error: bool,
        target_path: str,
        log: Any,
    ):
        self._default_flags = default_flags
        self._executable = executable
        self._warn_error = warn_error
        self._ignore_handled_error = ignore_handled_error
        self._target_path = target_path
        self._log = log

    @property
    def default_flags(self):
        # return a non-frozendict copy of self._default_flags

        def unfreeze(d):
            if isinstance(d, dict):
                return {k: unfreeze(v) for k, v in d.items()}
            return d

        return unfreeze(self._default_flags)

    def execute_command(self, command: str, extra_flags: Dict[str, Any] = None):
        command = check.str_param(command, "command")
        extra_flags = check.opt_dict_param(extra_flags, "extra_flags")

        return execute_cli(
            executable=self._executable,
            command=command,
            flags_dict=deep_merge_dicts(self.default_flags, extra_flags),
            log=self._log,
            warn_error=self._warn_error,
            ignore_handled_error=self._ignore_handled_error,
            target_path=self._target_path,
        )

    def run(self, extra_flags: Dict[str, Any] = None):
        return self.execute_command("run", extra_flags)

    def seed(self, extra_flags: Dict[str, Any] = None):
        return self.execute_command("seed", extra_flags)

    def test(self, extra_flags: Dict[str, Any] = None):
        return self.execute_command("test", extra_flags)

    def parse_run_results(self) -> Dict[str, Any]:
        return parse_run_results(self._default_flags["project-dir"], self._target_path)


@resource(
    config_schema={
        "default_flags": Field(
            config=Permissive(CLI_COMMON_FLAGS_CONFIG_SCHEMA),
            is_required=False,
            default_value={},
            description="The default set of flags that will be passed in to every CLI invocation.",
        ),
        **CLI_COMMON_OPTIONS_CONFIG_SCHEMA,
    },
    description="A resource that will run DBT CLI commands and parse results.",
)
def dbt_cli_resource(context):
    return DbtCliResource(
        executable=context.resource_config["dbt_executable"],
        default_flags=context.resource_config["default_flags"],
        warn_error=context.resource_config["warn-error"],
        ignore_handled_error=context.resource_config["ignore_handled_error"],
        target_path=context.resource_config["target-path"],
        log=context.log,
    )
