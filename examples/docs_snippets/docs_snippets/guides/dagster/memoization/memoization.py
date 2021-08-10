"""isort:skip_file"""


# start_op_strategy
from dagster import VersionStrategy
import inspect
import hashlib


class TrackOpChangesStrategy(VersionStrategy):
    def get_op_version(self, op_def):
        # Track changes in the op's source code
        code_as_str = inspect.getsource(op_def.compute_fn.decorated_fn)

        return hashlib.sha1(code_as_str.encode("utf-8")).hexdigest()


# end_op_strategy

# start_memoized_graph
from dagster import graph, op


@op
def emit_five():
    return 5


@op
def add_number(x):
    return x + 1


@graph
def emit_number():
    add_number(emit_five())


# end_memoized_graph

# start_memoized_job
emit_number_job = emit_number.to_job(version_strategy=TrackOpChangesStrategy())

# end_memoized_job

# start_version_stategy_resources
class TrackOpsandResourcesChangesStrategy(VersionStrategy):
    def _get_source_hash(self, fn):
        code_as_str = inspect.getsource(fn)
        return hashlib.sha1(code_as_str.encode("utf-8")).hexdigest()

    def get_op_version(self, op_def):
        # Track changes in the op's source code
        return self._get_source_hash(op_def.compute_fn.decorated_fn)

    def get_resource_version(self, resource_def):
        return self._get_source_hash(resource_def.resource_fn)


# end_version_strategy_resources