import json
import os
import pickle
import uuid

import dagstermill
from dagster import (
    Field,
    FileHandle,
    IOManager,
    InputDefinition,
    Int,
    List,
    ModeDefinition,
    Nothing,
    OutputDefinition,
    ResourceDefinition,
    String,
    fs_io_manager,
    io_manager,
    lambda_solid,
    pipeline,
    repository,
    resource,
    solid,
)
from dagster.core.storage.file_manager import local_file_manager
from dagster.utils import PICKLE_PROTOCOL, file_relative_path

try:
    from dagster_pandas import DataFrame

    DAGSTER_PANDAS_PRESENT = True
except ImportError:
    DAGSTER_PANDAS_PRESENT = False

try:
    import sklearn as _

    SKLEARN_PRESENT = True
except ImportError:
    SKLEARN_PRESENT = False

try:
    import matplotlib as _

    MATPLOTLIB_PRESENT = True
except ImportError:
    MATPLOTLIB_PRESENT = False


class BasicTest:
    def __init__(self, x):
        self.x = x

    def __repr__(self):
        return "BasicTest: {x}".format(x=str(self.x))


def nb_test_path(name):
    return file_relative_path(__file__, f"notebooks/{name}.ipynb")


def test_nb_solid(name, legacy=False, **kwargs):
    output_defs = kwargs.pop("output_defs", [OutputDefinition(is_required=False)])

    if legacy:
        return dagstermill.define_dagstermill_solid(
            name=f"{name}_legacy",
            notebook_path=nb_test_path(name),
            output_notebook="notebook",
            output_defs=output_defs,
            **kwargs,
        )
    else:
        return dagstermill.dagstermill_solid(
            name=name,
            notebook_path=nb_test_path(name),
            output_notebook="notebook",
            output_defs=output_defs,
            **kwargs,
        )


default_mode_defs = [
    ModeDefinition(
        "test", resource_defs={"file_manager": local_file_manager, "io_manager": fs_io_manager}
    ),
    ModeDefinition("legacy_test", resource_defs={"file_manager": local_file_manager}),
]


hello_world = test_nb_solid("hello_world", output_defs=[])

hello_world_legacy = test_nb_solid("hello_world", output_defs=[], legacy=True)


@pipeline(mode_defs=default_mode_defs)
def hello_world_pipeline():
    hello_world()


@pipeline(mode_defs=default_mode_defs)
def hello_world_pipeline_legacy():
    hello_world_legacy()


hello_world_config = test_nb_solid(
    "hello_world_config",
    config_schema={"greeting": Field(String, is_required=False, default_value="hello")},
)

hello_world_config_legacy = test_nb_solid(
    "hello_world_config",
    config_schema={"greeting": Field(String, is_required=False, default_value="hello")},
    legacy=True,
)


goodbye_config = dagstermill.dagstermill_solid(
    name="goodbye_config",
    notebook_path=nb_test_path("print_dagstermill_context_solid_config"),
    output_notebook="notebook",
    config_schema={"farewell": Field(String, is_required=False, default_value="goodbye")},
)

goodbye_config_legacy = dagstermill.define_dagstermill_solid(
    name="goodbye_config_legacy",
    notebook_path=nb_test_path("print_dagstermill_context_solid_config"),
    output_notebook="notebook",
    config_schema={"farewell": Field(String, is_required=False, default_value="goodbye")},
)


@pipeline(mode_defs=default_mode_defs)
def hello_world_config_pipeline():
    hello_world_config()
    goodbye_config()


@pipeline(mode_defs=default_mode_defs)
def hello_world_config_pipeline_legacy():
    hello_world_config_legacy()
    goodbye_config_legacy()


@solid(input_defs=[InputDefinition("notebook", dagster_type=FileHandle)])
def load_notebook_legacy(_, notebook):
    return os.path.exists(notebook.path_desc)


@solid(input_defs=[InputDefinition("notebook")])
def load_notebook(_, notebook):
    notebook_json = json.loads(str(notebook, encoding="utf-8"))
    return "cells" in notebook_json


@pipeline(mode_defs=default_mode_defs)
def hello_world_with_output_notebook_pipeline():
    notebook = hello_world()
    load_notebook(notebook)


@pipeline(mode_defs=default_mode_defs)
def hello_world_with_output_notebook_pipeline_legacy():
    notebook = hello_world_legacy()
    load_notebook_legacy(notebook)


hello_world_output = test_nb_solid("hello_world_output", output_defs=[OutputDefinition(str)])

hello_world_output_legacy = test_nb_solid(
    "hello_world_output", output_defs=[OutputDefinition(str)], legacy=True
)


@pipeline(mode_defs=default_mode_defs)
def hello_world_output_pipeline():
    hello_world_output()


@pipeline(mode_defs=default_mode_defs)
def hello_world_output_pipeline_legacy():
    hello_world_output_legacy()


hello_world_explicit_yield = test_nb_solid(
    "hello_world_explicit_yield", output_defs=[OutputDefinition(str)]
)


hello_world_explicit_yield_legacy = test_nb_solid(
    "hello_world_explicit_yield", output_defs=[OutputDefinition(str)], legacy=True
)


@pipeline(mode_defs=default_mode_defs)
def hello_world_explicit_yield_pipeline():
    hello_world_explicit_yield()


@pipeline(mode_defs=default_mode_defs)
def hello_world_explicit_yield_pipeline_legacy():
    hello_world_explicit_yield_legacy()


hello_logging = test_nb_solid("hello_logging")

hello_logging_legacy = test_nb_solid("hello_logging", legacy=True)


@pipeline(mode_defs=default_mode_defs)
def hello_logging_pipeline():
    hello_logging()


@pipeline(mode_defs=default_mode_defs)
def hello_logging_pipeline_legacy():
    hello_logging_legacy()


add_two_numbers = test_nb_solid(
    "add_two_numbers",
    input_defs=[
        InputDefinition(name="a", dagster_type=Int),
        InputDefinition(name="b", dagster_type=Int),
    ],
    output_defs=[OutputDefinition(Int)],
)

add_two_numbers_legacy = test_nb_solid(
    "add_two_numbers",
    input_defs=[
        InputDefinition(name="a", dagster_type=Int),
        InputDefinition(name="b", dagster_type=Int),
    ],
    output_defs=[OutputDefinition(Int)],
    legacy=True,
)


mult_two_numbers = test_nb_solid(
    "mult_two_numbers",
    input_defs=[
        InputDefinition(name="a", dagster_type=Int),
        InputDefinition(name="b", dagster_type=Int),
    ],
    output_defs=[OutputDefinition(Int)],
)

mult_two_numbers_legacy = test_nb_solid(
    "mult_two_numbers",
    input_defs=[
        InputDefinition(name="a", dagster_type=Int),
        InputDefinition(name="b", dagster_type=Int),
    ],
    output_defs=[OutputDefinition(Int)],
    legacy=True,
)


@lambda_solid
def return_one():
    return 1


@lambda_solid
def return_two():
    return 2


@pipeline(mode_defs=default_mode_defs)
def add_pipeline():
    add_two_numbers(return_one(), return_two())


@pipeline(mode_defs=default_mode_defs)
def add_pipeline_legacy():
    add_two_numbers_legacy(return_one(), return_two())


@solid(input_defs=[], config_schema=Int)
def load_constant(context):
    return context.solid_config


@pipeline(mode_defs=default_mode_defs)
def notebook_dag_pipeline():
    a = load_constant.alias("load_a")()
    b = load_constant.alias("load_b")()
    c, _ = add_two_numbers(a, b)
    mult_two_numbers(c, b)


@pipeline(mode_defs=default_mode_defs)
def notebook_dag_pipeline_legacy():
    a = load_constant.alias("load_a")()
    b = load_constant.alias("load_b")()
    c, _ = add_two_numbers_legacy(a, b)
    mult_two_numbers_legacy(c, b)


error_notebook = test_nb_solid("error_notebook")

error_notebook_legacy = test_nb_solid("error_notebook", legacy=True)


@pipeline(mode_defs=default_mode_defs)
def error_pipeline():
    error_notebook()


@pipeline(mode_defs=default_mode_defs)
def error_pipeline_legacy():
    error_notebook_legacy()


if DAGSTER_PANDAS_PRESENT and SKLEARN_PRESENT and MATPLOTLIB_PRESENT:

    clean_data = test_nb_solid("clean_data", output_defs=[OutputDefinition(DataFrame)])

    # TODO: add an output to this
    tutorial_LR = test_nb_solid(
        "tutorial_LR", input_defs=[InputDefinition(name="df", dagster_type=DataFrame)],
    )

    tutorial_LR_legacy = test_nb_solid(
        "tutorial_LR", input_defs=[InputDefinition(name="df", dagster_type=DataFrame)], legacy=True
    )

    tutorial_RF = test_nb_solid(
        "tutorial_RF", input_defs=[InputDefinition(name="df", dagster_type=DataFrame)],
    )

    tutorial_RF_legacy = test_nb_solid(
        "tutorial_RF", input_defs=[InputDefinition(name="df", dagster_type=DataFrame)], legacy=True
    )

    @pipeline(mode_defs=default_mode_defs)
    def tutorial_pipeline():
        df, _ = clean_data()
        # TODO: get better names for these
        tutorial_LR(df)
        tutorial_RF(df)

    @pipeline(mode_defs=default_mode_defs)
    def tutorial_pipeline_legacy():
        df, _ = clean_data()
        # TODO: get better names for these
        tutorial_LR_legacy(df)
        tutorial_RF_legacy(df)


@solid("resource_solid", required_resource_keys={"list"})
def resource_solid(context):
    context.resources.list.append("Hello, solid!")
    return True


hello_world_resource = test_nb_solid(
    "hello_world_resource", input_defs=[InputDefinition("nonce")], required_resource_keys={"list"},
)

hello_world_resource_legacy = test_nb_solid(
    "hello_world_resource",
    input_defs=[InputDefinition("nonce")],
    required_resource_keys={"list"},
    legacy=True,
)

hello_world_resource_with_exception = test_nb_solid(
    "hello_world_resource_with_exception",
    input_defs=[InputDefinition("nonce")],
    required_resource_keys={"list"},
)

hello_world_resource_with_exception_legacy = test_nb_solid(
    "hello_world_resource_with_exception",
    input_defs=[InputDefinition("nonce")],
    required_resource_keys={"list"},
    legacy=True,
)


class FilePickleList:
    # This is not thread- or anything else-safe
    def __init__(self, path):
        self.closed = False
        self.id = str(uuid.uuid4())[-6:]
        self.path = path
        self.list = []
        if not os.path.exists(self.path):
            self.write()
        self.read()
        self.open()

    def open(self):
        self.read()
        self.append("Opened")

    def append(self, obj):
        self.read()
        self.list.append(self.id + ": " + obj)
        self.write()

    def read(self):
        with open(self.path, "rb") as fd:
            self.list = pickle.load(fd)
            return self.list

    def write(self):
        with open(self.path, "wb") as fd:
            pickle.dump(self.list, fd, protocol=PICKLE_PROTOCOL)

    def close(self):
        self.append("Closed")
        self.closed = True


@resource(config_schema=Field(String))
def filepicklelist_resource(init_context):
    filepicklelist = FilePickleList(init_context.resource_config)
    try:
        yield filepicklelist
    finally:
        filepicklelist.close()


@pipeline(
    mode_defs=[
        ModeDefinition(
            name="test",
            resource_defs={
                "list": ResourceDefinition(lambda _: []),
                "file_manager": local_file_manager,
                "io_manager": fs_io_manager,
            },
        ),
        ModeDefinition(
            name="prod",
            resource_defs={
                "list": filepicklelist_resource,
                "file_manager": local_file_manager,
                "io_manager": fs_io_manager,
            },
        ),
    ]
)
def resource_pipeline():
    hello_world_resource(resource_solid())


@pipeline(
    mode_defs=[
        ModeDefinition(
            name="test",
            resource_defs={
                "list": ResourceDefinition(lambda _: []),
                "file_manager": local_file_manager,
            },
        ),
        ModeDefinition(
            name="prod",
            resource_defs={"list": filepicklelist_resource, "file_manager": local_file_manager,},
        ),
    ]
)
def resource_pipeline_legacy():
    hello_world_resource_legacy(resource_solid())


@pipeline(
    mode_defs=[
        ModeDefinition(
            resource_defs={
                "list": filepicklelist_resource,
                "file_manager": local_file_manager,
                "io_manager": fs_io_manager,
            }
        )
    ]
)
def resource_with_exception_pipeline():
    hello_world_resource_with_exception(resource_solid())


@pipeline(
    mode_defs=[
        ModeDefinition(
            resource_defs={"list": filepicklelist_resource, "file_manager": local_file_manager,}
        )
    ]
)
def resource_with_exception_pipeline_legacy():
    hello_world_resource_with_exception_legacy(resource_solid())


bad_kernel = test_nb_solid("bad_kernel")


bad_kernel_legacy = test_nb_solid("bad_kernel", legacy=True)


@pipeline(mode_defs=default_mode_defs)
def bad_kernel_pipeline():
    bad_kernel()


@pipeline(mode_defs=default_mode_defs)
def bad_kernel_pipeline_legacy():
    bad_kernel_legacy()


reimport = test_nb_solid(
    "reimport", input_defs=[InputDefinition("l", List[int])], output_defs=[OutputDefinition(int)]
)


reimport_legacy = test_nb_solid(
    "reimport",
    input_defs=[InputDefinition("l", List[int])],
    output_defs=[OutputDefinition(int)],
    legacy=True,
)


@solid
def lister(_):
    return [1, 2, 3]


@pipeline(mode_defs=default_mode_defs)
def reimport_pipeline():
    reimport(lister())


@pipeline(mode_defs=default_mode_defs)
def reimport_pipeline_legacy():
    reimport_legacy(lister())


yield_3 = test_nb_solid("yield_3", output_defs=[OutputDefinition(Int)])

yield_3_legacy = test_nb_solid("yield_3", output_defs=[OutputDefinition(Int)], legacy=True)


@pipeline(mode_defs=default_mode_defs)
def yield_3_pipeline():
    yield_3()


@pipeline(mode_defs=default_mode_defs)
def yield_3_pipeline_legacy():
    yield_3_legacy()


yield_obj = test_nb_solid("yield_obj")

yield_obj_legacy = test_nb_solid("yield_obj", legacy=True)


@pipeline(mode_defs=default_mode_defs)
def yield_obj_pipeline():
    yield_obj()


@pipeline(mode_defs=default_mode_defs)
def yield_obj_pipeline_legacy():
    yield_obj_legacy()


sum_nums = test_nb_solid("sum_nums", input_defs=[InputDefinition("nums", List[int])])

sum_nums_legacy = test_nb_solid(
    "sum_nums", input_defs=[InputDefinition("nums", List[int])], legacy=True
)


def yield_num(num):
    @solid(name=f"yield_{num}")
    def _yield_num(_) -> int:
        return num

    return _yield_num


@pipeline(mode_defs=default_mode_defs)
def sum_nums_pipeline():
    a = yield_num(5)()
    b = yield_num(6)()
    c = yield_num(7)()
    sum_nums([a, b, c])


@pipeline(mode_defs=default_mode_defs)
def sum_nums_pipeline_legacy():
    a = yield_num(5)()
    b = yield_num(6)()
    c = yield_num(7)()
    sum_nums_legacy([a, b, c])


nothing_solid = test_nb_solid(
    "nothing", input_defs=[InputDefinition("num", int), InputDefinition("nothing", Nothing)]
)


@pipeline(mode_defs=default_mode_defs)
def nothing_pipeline():
    num = yield_num(6)()
    nothing = yield_num(7)()
    nothing_solid(num=num, nothing=nothing)


hello_world_custom_output_notebook_io_manager = test_nb_solid(
    "hello_world", output_notebook_io_manager_key="file_io_manager"
)


class FileIOManager(IOManager):
    def __init__(self, file_target):
        self.file_target = file_target

    def load_input(self, context):
        with open(self.file_target, "rb") as fd:
            return fd.read()

    def handle_output(self, context, obj):
        with open(self.file_target, "wb") as fd:
            fd.write(obj)


@io_manager(config_schema={"file_target": str})
def file_io_manager(context):
    return FileIOManager(file_target=context.resource_config["file_target"])


@pipeline(
    mode_defs=[
        ModeDefinition(
            "test",
            resource_defs={"io_manager": fs_io_manager, "file_io_manager": file_io_manager,},
        )
    ]
)
def custom_output_notebook_io_manager_pipeline():
    hello_world_custom_output_notebook_io_manager()


class AlwaysSevenIOManager(IOManager):
    def load_input(self, context):
        return 7

    def handle_output(self, context, obj):
        return


@io_manager
def seven_io_manager(_context):
    return AlwaysSevenIOManager()


add_two_numbers_always_7 = test_nb_solid(
    "add_two_numbers",
    input_defs=[
        InputDefinition(name="a", dagster_type=Int),
        InputDefinition(name="b", dagster_type=Int),
    ],
    output_defs=[OutputDefinition(Int, io_manager_key="seven_io_manager")],
)


@pipeline(
    mode_defs=[
        ModeDefinition(
            "test",
            resource_defs={"io_manager": fs_io_manager, "seven_io_manager": seven_io_manager,},
        )
    ]
)
def custom_output_manager_pipeline():
    add_two_numbers_always_7(a=yield_num(3)(), b=yield_num(10)())


@repository
def notebook_repo():
    pipelines = [
        add_pipeline,
        bad_kernel_pipeline,
        custom_output_manager_pipeline,
        error_pipeline,
        hello_logging_pipeline,
        hello_world_config_pipeline,
        hello_world_explicit_yield_pipeline,
        hello_world_output_pipeline,
        hello_world_pipeline,
        notebook_dag_pipeline,
        nothing_pipeline,
        reimport_pipeline,
        resource_pipeline,
        resource_with_exception_pipeline,
        sum_nums_pipeline,
        yield_3_pipeline,
        yield_obj_pipeline,
    ]
    if DAGSTER_PANDAS_PRESENT and SKLEARN_PRESENT and MATPLOTLIB_PRESENT:
        pipelines += [tutorial_pipeline]

    return pipelines
