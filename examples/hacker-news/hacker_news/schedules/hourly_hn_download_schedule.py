from datetime import datetime

from dagster import hourly_schedule


def get_hourly_download_def_schedule_config(start_time: datetime):
    return {
        "resources": {
            "partition_start": {"config": start_time.strftime("%Y-%m-%d %H:00:00")},
            "partition_end": {"config": start_time.strftime("%Y-%m-%d %H:59:59")},
        }
    }


def get_hourly_dynamic_download_def_schedule_config(start_time: datetime):
    return {
        "resources": {
            "partition_start": {"config": start_time.strftime("%Y-%m-%d %H:00:00")},
            "partition_end": {"config": start_time.strftime("%Y-%m-%d %H:59:59")},
        },
        "execution": {"multiprocess": {"config": {"max_concurrent": 32}}},
        "solids": {
            "dynamic_id_ranges_for_time": {
                "config": {
                    "batch_size": 100,
                }
            }
        },
    }


def get_schedule_for_mode(
    mode_name: str,
    schedule_name: str,
    config_fn=get_hourly_download_def_schedule_config,
    pipeline_name="download_pipeline",
):
    @hourly_schedule(
        name=schedule_name,
        pipeline_name=pipeline_name,
        start_date=datetime(2021, 1, 1),
        execution_timezone="UTC",
        mode=mode_name,
    )
    def _schedule(date):
        return config_fn(date)

    return _schedule


hourly_hn_download_staging_schedule = get_schedule_for_mode(
    "staging_live_data", "hourly_hn_download_staging"
)
hourly_hn_download_prod_schedule = get_schedule_for_mode("prod", "hourly_hn_download_prod")
hourly_hn_dynamic_download_prod_schedule = get_schedule_for_mode(
    "prod",
    "hourly_hn_download_dynamic_prod",
    config_fn=get_hourly_dynamic_download_def_schedule_config,
    pipeline_name="dynamic_download_pipeline",
)
