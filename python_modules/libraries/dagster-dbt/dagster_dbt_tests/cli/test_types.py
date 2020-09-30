from dagster_dbt import CliRunResult

from ..test_results import RUN_RESULT_DICT

CLI_RUN_RESULT_DICT = {
    "return_code": 0,
    "raw_output": "The raw output (stdout).",
    **RUN_RESULT_DICT,
}

CLI_RUN_RESULT_WITH_STATS_DICT = {
    "num_pass": 1,
    "num_warn": 1,
    "num_error": 1,
    "num_skip": 1,
    "num_total": 4,
    **CLI_RUN_RESULT_DICT,
}


class TestCliRunResult:
    def test_from_dict(self):
        crr = CliRunResult.from_dict(CLI_RUN_RESULT_DICT)
        assert len(crr) == len(CLI_RUN_RESULT_DICT["results"])
        assert crr.num_pass is None
        assert crr.num_warn is None
        assert crr.num_error is None
        assert crr.num_skip is None
        assert crr.num_total is None

    def test_from_dict_with_stats(self):
        crr = CliRunResult.from_dict(CLI_RUN_RESULT_WITH_STATS_DICT)
        assert len(crr) == len(CLI_RUN_RESULT_WITH_STATS_DICT["results"])
        assert crr.num_pass == CLI_RUN_RESULT_WITH_STATS_DICT["num_pass"]
        assert crr.num_warn == CLI_RUN_RESULT_WITH_STATS_DICT["num_warn"]
        assert crr.num_error == CLI_RUN_RESULT_WITH_STATS_DICT["num_error"]
        assert crr.num_skip == CLI_RUN_RESULT_WITH_STATS_DICT["num_skip"]
        assert crr.num_total == CLI_RUN_RESULT_WITH_STATS_DICT["num_total"]
