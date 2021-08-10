"""isort:skip_file"""
# pylint: disable=reimported

# start_op_strategy
from dagster import VersionStrategy
import inspect
import hashlib


class OpSourceHashStrategy(VersionStrategy):
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
emit_number_job = emit_number.to_job(version_strategy=OpSourceHashStrategy())

# end_memoized_job

# start_graph_reqs_resources
from dagster import graph, op


@op(required_resource_keys={"foo", "bar"})
def foobar_op():
    pass


@graph
def foobar_graph():
    foobar_op()


# end_graph_reqs_resources

# start_version_strategy_resources
from dagster import VersionStrategy


class OpAndResourcesSourceHashStrategy(VersionStrategy):
    def _get_source_hash(self, fn):
        code_as_str = inspect.getsource(fn)
        return hashlib.sha1(code_as_str.encode("utf-8")).hexdigest()

    def get_op_version(self, op_def):
        # Track changes in the op's source code
        return self._get_source_hash(op_def.compute_fn.decorated_fn)

    def get_resource_version(self, resource_key, resource_def):
        if resource_key == "foo":
            return self._get_source_hash(resource_def.resource_fn)
        else:
            return None


# end_version_strategy_resources

# start_disable_memoization
from dagster import MEMOIZED_RUN_TAG

disabled_result = emit_number_job.execute_in_process(tags={MEMOIZED_RUN_TAG: "false"})
# end_disable_memoization

# start_pipeline_memoization
from dagster import pipeline, solid, VersionStrategy, fs_io_manager, ModeDefinition


class SolidSourceHashStrategy(VersionStrategy):
    def get_solid_version(self, solid_def):
        # Track changes in the op's source code
        code_as_str = inspect.getsource(solid_def.compute_fn.decorated_fn)

        return hashlib.sha1(code_as_str.encode("utf-8")).hexdigest()


@solid
def emit_five_solid():
    return 5


@pipeline(
    mode_defs=[ModeDefinition(resource_defs={"io_manager": fs_io_manager})],
    version_strategy=SolidSourceHashStrategy(),
)
def emit_number_pipeline():
    emit_five()


# end_pipeline_memoization
