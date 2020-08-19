from typing import List

from dagster_dbt import DbtRpcPollResult, dbt_rpc_resource, dbt_rpc_run_and_wait

from dagster import ModeDefinition, configured, execute_solid


def get_executed_models(resp: DbtRpcPollResult) -> List[str]:
    assert isinstance(resp.results, list)
    return set(res.node['alias'] for res in resp.results)


def output_for_solid_executed_with_rpc_resource(a_solid) -> DbtRpcPollResult:
    mode_def = ModeDefinition(resource_defs={'dbt_rpc': dbt_rpc_resource})  # use config defaults
    solid_result = execute_solid(a_solid, mode_def)

    assert solid_result.success
    output = solid_result.output_value()
    assert isinstance(output, DbtRpcPollResult)
    return output


class TestDBTRunAndWait:
    def test_run_all(self, dbt_rpc_server):  # pylint: disable=unused-argument
        run_all_fast_poll = configured(dbt_rpc_run_and_wait, name='run_all_fast_poll')(
            {'interval': 2}
        )

        dbt_result = output_for_solid_executed_with_rpc_resource(run_all_fast_poll)

        assert get_executed_models(dbt_result) == {
            'least_caloric',
            'sort_by_calories',
            'sort_cold_cereals_by_calories',
            'sort_hot_cereals_by_calories',
        }

    def test_run_single(self, dbt_rpc_server):  # pylint: disable=unused-argument
        run_single_fast_poll = configured(dbt_rpc_run_and_wait, name='run_least_caloric')(
            {'interval': 2, 'models': ['least_caloric']}
        )

        dbt_result = output_for_solid_executed_with_rpc_resource(run_single_fast_poll)

        assert get_executed_models(dbt_result) == {'least_caloric'}
