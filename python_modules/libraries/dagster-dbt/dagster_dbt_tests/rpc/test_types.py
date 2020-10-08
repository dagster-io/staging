from dagster_dbt import DbtRpcResult

from ..test_types import DBT_RESULT_DICT

DBT_RPC_RESULT_DICT = {
    **DBT_RESULT_DICT,
    "state": "success",
    "start": "2020-09-28T17:10:56.070900Z",
    "end": "2020-09-28T17:10:58.116186Z",
    "elapsed": 2.045286,
}


class TestRpcRunResult:
    def test_from_dict(self):
        prr = DbtRpcResult.from_dict(DBT_RPC_RESULT_DICT)
        assert len(prr) == len(DBT_RPC_RESULT_DICT["results"])
