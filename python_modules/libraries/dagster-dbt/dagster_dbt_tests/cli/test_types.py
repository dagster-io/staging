from dagster_dbt import DbtCliResult

from ..test_types import DBT_RESULT_DICT

DBT_CLI_RESULT_DICT = {
    "command": "dbt run",
    "return_code": 0,
    "raw_output": "The raw output (stdout).",
    **DBT_RESULT_DICT,
}

DBT_CLI_RESULT_WITH_STATS_DICT = {
    "num_pass": 1,
    "num_warn": 1,
    "num_error": 1,
    "num_skip": 1,
    "num_total": 4,
    **DBT_CLI_RESULT_DICT,
}


class TestCliRunResult:
    def test_from_dict(self):
        crr = DbtCliResult.from_dict(DBT_CLI_RESULT_DICT)
        assert len(crr) == len(DBT_CLI_RESULT_DICT["results"])
        assert crr.num_pass is None
        assert crr.num_warn is None
        assert crr.num_error is None
        assert crr.num_skip is None
        assert crr.num_total is None

    def test_from_dict_with_stats(self):
        crr = DbtCliResult.from_dict(DBT_CLI_RESULT_WITH_STATS_DICT)
        assert len(crr) == len(DBT_CLI_RESULT_WITH_STATS_DICT["results"])
        assert crr.num_pass == DBT_CLI_RESULT_WITH_STATS_DICT["num_pass"]
        assert crr.num_warn == DBT_CLI_RESULT_WITH_STATS_DICT["num_warn"]
        assert crr.num_error == DBT_CLI_RESULT_WITH_STATS_DICT["num_error"]
        assert crr.num_skip == DBT_CLI_RESULT_WITH_STATS_DICT["num_skip"]
        assert crr.num_total == DBT_CLI_RESULT_WITH_STATS_DICT["num_total"]
