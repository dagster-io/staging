from typing import Any, Dict, Optional

from dagster import check
from dagster.core.definitions import PipelineDefinition
from dagster.core.system_config.objects import EnvironmentConfig


def validate_run_config(
    pipeline_def: PipelineDefinition,
    run_config: Optional[Dict[str, Any]] = None,
    mode: Optional[str] = None,
) -> None:
    pipeline_def = check.inst_param(pipeline_def, "pipeline_def", PipelineDefinition)
    run_config = check.opt_dict_param(run_config, "run_config", key_type=str)
    mode = check.opt_str_param(mode, "mode", default=pipeline_def.get_default_mode_name())

    EnvironmentConfig.build(pipeline_def, run_config, mode=mode)
