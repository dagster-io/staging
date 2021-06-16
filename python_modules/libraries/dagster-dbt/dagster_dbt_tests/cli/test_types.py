import pickle

from dagster_dbt import DbtCliOutput
from dagster_dbt.types import DbtResult

from ..test_types import DBT_RESULT_DICT


class TestDbtCliOutput:
    def test_init(self):
        dco = DbtCliOutput(
            command="dbt run",
            return_code=0,
            raw_output="The raw output (stdout).",
            result=DbtResult.from_run_results(DBT_RESULT_DICT),
            logs=[],
        )
        assert len(dco.result) == len(DBT_RESULT_DICT["results"])

    def test_pickle_roundtrip(self):  # pylint: disable=unused-argument
        dco = DbtCliOutput(
            command="dbt run",
            return_code=0,
            raw_output="The raw output (stdout).",
            result=DbtResult.from_run_results(DBT_RESULT_DICT),
            logs=[{"some": {"nested": {"logs"}}}, {"other": "log"}],
        )
        assert pickle.loads(pickle.dumps(dco)) == dco
