import concurrent.futures
import subprocess
import tempfile
import time

from dagster import execute_pipeline
from user_in_loop.user_in_loop.repo import user_in_the_loop_pipeline


def run_pipeline(path_to_dir):
    result = execute_pipeline(
        user_in_the_loop_pipeline,
        # start_loop_marker_2
        run_config={
            "solids": {"wait_for_user_approval": {"config": {"file": f"{path_to_dir}/data.csv"}}}
        },
    )
    # end_loop_marker_2
    return result


def create_file(path_to_dir):
    time.sleep(1)
    proc = subprocess.Popen(["touch", f"{path_to_dir}/data.csv"])
    proc.wait()


def test_user_in_loop_pipeline(capsys):
    with tempfile.TemporaryDirectory(dir="user_in_loop") as temp_dir:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            f1 = executor.submit(run_pipeline, temp_dir)
            executor.submit(create_file, temp_dir)

            pipeline_run_res = f1.result()
            assert pipeline_run_res.success

    res_one = False
    res_two = False
    res_three = False

    captured = capsys.readouterr()
    for line in captured.err.split("\n"):
        if line:
            if "Condition is met" in line:
                res_one = True

            if "Condition not met" in line:
                res_two = True

            if "The final price list is [5.15, 10.3, 12.36]" in line:
                res_three = True

    assert res_one
    assert res_two
    assert res_three
