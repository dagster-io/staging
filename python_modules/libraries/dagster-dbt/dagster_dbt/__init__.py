from dagster.core.utils import check_dagster_package_version

from .resources import dbt_rpc_resource  # , DbtRpcClient
from .types import DbtRpcPollResult
from .version import __version__

from .solids import (  # create_dbt_rpc_run_sql_solid,; dbt_rpc_compile_sql,; dbt_rpc_poll,; dbt_rpc_run,; dbt_rpc_run_operation,; dbt_rpc_run_operation_and_wait,; dbt_rpc_snapshot,; dbt_rpc_snapshot_and_wait,; dbt_rpc_snapshot_freshness,; dbt_rpc_snapshot_freshness_and_wait,; dbt_rpc_test,; dbt_rpc_test_and_wait,
    dbt_rpc_run_and_wait,
)

check_dagster_package_version('dagster-dbt', __version__)

__all__ = ['dbt_rpc_resource', 'dbt_rpc_run_and_wait', 'DbtRpcPollResult']
