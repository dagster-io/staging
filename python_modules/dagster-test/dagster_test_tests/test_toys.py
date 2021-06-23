import pytest
from dagster import (
    DagsterResourceFunctionError,
    DagsterTypeCheckDidNotPass,
    execute_pipeline,
    reconstructable,
)
from dagster.core.test_utils import instance_for_test
from dagster.utils import file_relative_path
from dagster.utils.temp_file import get_temp_dir
from dagster_test.toys.asset_lineage import asset_lineage_pipeline
from dagster_test.toys.composition import composition_graph
from dagster_test.toys.dynamic import dynamic_pipeline
from dagster_test.toys.error_monster import error_monster_job
from dagster_test.toys.hammer import hammer_pipeline
from dagster_test.toys.log_spew import log_spew_job
from dagster_test.toys.longitudinal import IntentionalRandomFailure, longitudinal_graph
from dagster_test.toys.many_events import many_events_job
from dagster_test.toys.pyspark_assets.pyspark_assets_pipeline import pyspark_assets_pipeline
from dagster_test.toys.repo import toys_repository
from dagster_test.toys.resources import resource_job
from dagster_test.toys.retries import retry_job
from dagster_test.toys.schedules import longitudinal_schedule
from dagster_test.toys.sleepy import sleepy_job


@pytest.mark.skip("schedules not cragified")
def test_repo():
    assert toys_repository


def test_dynamic_pipeline():
    assert execute_pipeline(dynamic_pipeline).success


def test_longitudinal_graph():
    partition_set = longitudinal_schedule().get_partition_set()
    try:
        result = execute_pipeline(
            longitudinal_graph.to_job(),
            run_config=partition_set.run_config_for_partition(partition_set.get_partitions()[0]),
        )
        assert result.success
    except IntentionalRandomFailure:
        pass


def test_many_events_graph():
    assert execute_pipeline(many_events_job).success


def get_sleepy_job():
    return sleepy_job


def test_sleepy_job():
    with instance_for_test() as instance:
        assert execute_pipeline(reconstructable(get_sleepy_job), instance=instance).success


def test_spew_job():
    assert execute_pipeline(log_spew_job).success


def test_hammer_pipeline():
    assert execute_pipeline(hammer_pipeline).success


def test_resource_job_no_config():
    result = execute_pipeline(resource_job)
    assert result.result_for_solid("one").output_value() == 2


def test_resource_job_with_config():
    result = execute_pipeline(resource_job, run_config={"resources": {"R1": {"config": 2}}})
    assert result.result_for_solid("one").output_value() == 3


def test_pyspark_assets_pipeline():
    with get_temp_dir() as temp_dir:
        run_config = {
            "solids": {
                "get_max_temp_per_station": {
                    "config": {
                        "temperature_file": "temperature.csv",
                        "version_salt": "foo",
                    }
                },
                "get_consolidated_location": {
                    "config": {
                        "station_file": "stations.csv",
                        "version_salt": "foo",
                    }
                },
                "combine_dfs": {
                    "config": {
                        "version_salt": "foo",
                    }
                },
                "pretty_output": {
                    "config": {
                        "version_salt": "foo",
                    }
                },
            },
            "resources": {
                "source_data_dir": {
                    "config": {
                        "dir": file_relative_path(
                            __file__, "../dagster_test/toys/pyspark_assets/asset_pipeline_files"
                        ),
                    }
                },
                "savedir": {"config": {"dir": temp_dir}},
            },
        }

        result = execute_pipeline(
            pyspark_assets_pipeline,
            run_config=run_config,
        )
        assert result.success


def test_error_monster_graph_success():
    assert execute_pipeline(
        error_monster_job,
        run_config={
            "solids": {
                "start": {"config": {"throw_in_solid": False, "return_wrong_type": False}},
                "middle": {"config": {"throw_in_solid": False, "return_wrong_type": False}},
                "end": {"config": {"throw_in_solid": False, "return_wrong_type": False}},
            },
            "resources": {"errorable_resource": {"config": {"throw_on_resource_init": False}}},
        },
    ).success


def test_error_monster_success_error_on_resource():
    with pytest.raises(DagsterResourceFunctionError):
        execute_pipeline(
            error_monster_job,
            run_config={
                "solids": {
                    "start": {"config": {"throw_in_solid": False, "return_wrong_type": False}},
                    "middle": {"config": {"throw_in_solid": False, "return_wrong_type": False}},
                    "end": {"config": {"throw_in_solid": False, "return_wrong_type": False}},
                },
                "resources": {"errorable_resource": {"config": {"throw_on_resource_init": True}}},
            },
        )


def test_error_monster_type_error():
    with pytest.raises(DagsterTypeCheckDidNotPass):
        execute_pipeline(
            error_monster_job,
            run_config={
                "solids": {
                    "start": {"config": {"throw_in_solid": False, "return_wrong_type": False}},
                    "middle": {"config": {"throw_in_solid": False, "return_wrong_type": True}},
                    "end": {"config": {"throw_in_solid": False, "return_wrong_type": False}},
                },
                "resources": {"errorable_resource": {"config": {"throw_on_resource_init": False}}},
            },
        )


def test_composition_job():
    result = execute_pipeline(
        composition_graph.to_job(),
        run_config={"solids": {"add_four": {"inputs": {"num": 3}}}},
    )

    assert result.success

    assert result.output_for_solid("div_four") == 7.0 / 4.0


def test_asset_lineage_pipeline():
    assert execute_pipeline(
        asset_lineage_pipeline,
        run_config={
            "solids": {
                "download_data": {"outputs": {"result": {"partitions": ["2020-01-01"]}}},
                "split_action_types": {
                    "outputs": {
                        "comments": {"partitions": ["2020-01-01"]},
                        "reviews": {"partitions": ["2020-01-01"]},
                    }
                },
                "top_10_comments": {"outputs": {"result": {"partitions": ["2020-01-01"]}}},
                "top_10_reviews": {"outputs": {"result": {"partitions": ["2020-01-01"]}}},
            }
        },
    ).success


def test_retry_job():
    assert execute_pipeline(retry_job).success
