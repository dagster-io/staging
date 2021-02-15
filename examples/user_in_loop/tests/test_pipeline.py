import concurrent.futures
import json
import subprocess
import time

from dagster import execute_pipeline
from user_in_loop.repo import user_in_the_loop_pipeline


def run_pipeline():
    result = execute_pipeline(
        user_in_the_loop_pipeline,
        run_config={"loggers": {"my_json_logger": {"config": {"log_level": "INFO"}}}},
    )
    return result


def create_file():
    time.sleep(1)
    proc = subprocess.Popen(["touch", "data.csv"])
    proc.wait()


def test_wait_for_user_approval(capsys):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        f1 = executor.submit(run_pipeline)
        f2 = executor.submit(create_file)

        pipeline_run_res = f1.result()
        assert pipeline_run_res.success

    res_one = False
    res_two = False
    res_three = False

    captured = capsys.readouterr()
    print(captured)

    for line in captured.err.split("\n"):
        if line:
            parsed = json.loads(line)
            if (
                parsed["dagster_meta"]["orig_message"]
                == "Condition not met. Check again in five seconds."
            ):
                res_one = True
            if parsed["dagster_meta"]["orig_message"] == "Condition is met.":
                res_two = True
            if (
                parsed["dagster_meta"]["orig_message"]
                == "The final price list is [10.725, 26.8125, 42.9]"
            ):
                res_three = True

    assert res_one
    assert res_two
    assert res_three

    proc = subprocess.Popen(["rm", "data.csv"])
    proc.wait()
