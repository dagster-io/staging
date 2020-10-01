from dagster_dbt import RpcRunResult

from ..test_results import RUN_RESULT_DICT

POLLED_RUN_RESULT_DICT = {
    **RUN_RESULT_DICT,
    "state": "success",
    "start": "2020-09-28T17:10:56.070900Z",
    "end": "2020-09-28T17:10:58.116186Z",
    "elapsed": 2.045286,
}


class TestRpcRunResult:
    def test_from_dict(self):
        prr = RpcRunResult.from_dict(POLLED_RUN_RESULT_DICT)
        assert len(prr) == len(POLLED_RUN_RESULT_DICT["results"])
