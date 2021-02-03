import os
import sqlite3
from tempfile import TemporaryDirectory

import pandas as pd
from dagster import execute_pipeline

from ..repo import my_pipeline


def test_pandas_lake():
    with TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "my_db.sqlite")

        # populate source data
        with sqlite3.connect(db_path) as con:
            people_df = pd.DataFrame([{"name": "frank", "age": 40}])
            people_df.to_sql("people", con)

        assert execute_pipeline(
            my_pipeline,
            run_config={
                "resources": {
                    "io_manager": {"config": {"db_path": db_path}},
                    "root_manager": {"config": {"db_path": db_path}},
                }
            },
        ).success
