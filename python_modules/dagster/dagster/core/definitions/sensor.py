from dagster import check
from dagster.core.definitions.schedule import ScheduleDefinition
from dagster.utils.backcompat import experimental_class_warning


class SensorDefinition(ScheduleDefinition):
    def __init__(
        self,
        name,
        pipeline_name,
        should_execute_fn,
        run_config_fn=None,
        tags_fn=None,
        solid_selection=None,
        mode="default",
        environment_vars=None,
    ):
        experimental_class_warning("SensorDefinition")

        super(SensorDefinition, self).__init__(
            name=check.str_param(name, "name"),
            cron_schedule="* * * * *",
            pipeline_name=check.str_param(pipeline_name, "pipeline_name"),
            should_execute=check.callable_param(should_execute_fn, "should_execute_fn"),
            run_config_fn=check.opt_callable_param(run_config_fn, "run_config_fn"),
            tags_fn=check.opt_callable_param(tags_fn, "tags_fn"),
            solid_selection=check.opt_nullable_list_param(
                solid_selection, "solid_selection", of_type=str
            ),
            mode=check.opt_str_param(mode, "mode"),
            environment_vars=check.opt_dict_param(
                environment_vars, "environment_vars", key_type=str, value_type=str
            ),
        )
