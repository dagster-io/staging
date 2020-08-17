from functools import update_wrapper

from dagster import check
from dagster.core.storage.type_storage import TypeStoragePlugin
from dagster.utils.backcompat import canonicalize_backcompat_args

from .config_schema import DagsterTypeLoader, DagsterTypeMaterializer
from .dagster_type import (
    DagsterType,
    DagsterTypeKind,
    PythonObjectDagsterType,
    make_python_type_usable_as_dagster_type,
    validate_type_check_fn,
)
from .marshal import PickleSerializationStrategy, SerializationStrategy


def usable_as_dagster_type(
    name=None,
    description=None,
    loader=None,
    materializer=None,
    serialization_strategy=None,
    auto_plugins=None,
    # Graveyard is below
    input_hydration_config=None,
    output_materialization_config=None,
):
    '''Decorate a Python class to make it usable as a Dagster Type.

    This is intended to make it straightforward to annotate existing business logic classes to
    make them dagster types whose typecheck is an isinstance check against that python class.

    Args:
        python_type (cls): The python type to make usable as python type.
        name (Optional[str]): Name of the new Dagster type. If ``None``, the name (``__name__``) of
            the ``python_type`` will be used.
        description (Optional[str]): A user-readable description of the type.
        loader (Optional[DagsterTypeLoader]): An instance of a class that
            inherits from :py:class:`DagsterTypeLoader` and can map config data to a value of
            this type. Specify this argument if you will need to shim values of this type using the
            config machinery. As a rule, you should use the
            :py:func:`@dagster_type_loader <dagster.dagster_type_loader>` decorator to construct
            these arguments.
        materializer (Optional[DagsterTypeMaterializer]): An instance of a class
            that inherits from :py:class:`DagsterTypeMaterializer` and can persist values of
            this type. As a rule, you should use the
            :py:func:`@dagster_type_materializer <dagster.dagster_type_materializer>`
            decorator to construct these arguments.
        serialization_strategy (Optional[SerializationStrategy]): An instance of a class that
            inherits from :py:class:`SerializationStrategy`. The default strategy for serializing
            this value when automatically persisting it between execution steps. You should set
            this value if the ordinary serialization machinery (e.g., pickle) will not be adequate
            for this type.
        auto_plugins (Optional[List[TypeStoragePlugin]]): If types must be serialized differently
            depending on the storage being used for intermediates, they should specify this
            argument. In these cases the serialization_strategy argument is not sufficient because
            serialization requires specialized API calls, e.g. to call an S3 API directly instead
            of using a generic file object. See ``dagster_pyspark.DataFrame`` for an example.

    Examples:

    .. code-block:: python

        # dagster_aws.s3.file_manager.S3FileHandle
        @usable_as_dagster_type
        class S3FileHandle(FileHandle):
            def __init__(self, s3_bucket, s3_key):
                self._s3_bucket = check.str_param(s3_bucket, 's3_bucket')
                self._s3_key = check.str_param(s3_key, 's3_key')

            @property
            def s3_bucket(self):
                return self._s3_bucket

            @property
            def s3_key(self):
                return self._s3_key

            @property
            def path_desc(self):
                return self.s3_path

            @property
            def s3_path(self):
                return 's3://{bucket}/{key}'.format(bucket=self.s3_bucket, key=self.s3_key)
        '''

    def _with_args(bare_cls):
        check.type_param(bare_cls, 'bare_cls')
        new_name = name if name else bare_cls.__name__

        make_python_type_usable_as_dagster_type(
            bare_cls,
            PythonObjectDagsterType(
                name=new_name,
                description=description,
                python_type=bare_cls,
                loader=canonicalize_backcompat_args(
                    loader, 'loader', input_hydration_config, 'input_hydration_config', '0.10.0',
                ),
                materializer=canonicalize_backcompat_args(
                    materializer,
                    'materializer',
                    output_materialization_config,
                    'output_materialization_config',
                    '0.10.0',
                ),
                serialization_strategy=serialization_strategy,
                auto_plugins=auto_plugins,
            ),
        )
        return bare_cls

    # check for no args, no parens case
    if callable(name):
        bare_cls = name  # with no parens, name is actually the decorated class
        make_python_type_usable_as_dagster_type(
            bare_cls,
            PythonObjectDagsterType(python_type=bare_cls, name=bare_cls.__name__, description=None),
        )
        return bare_cls

    return _with_args


class _DagsterType(object):
    def __init__(
        self,
        name=None,
        description=None,
        loader=None,
        materializer=None,
        serialization_strategy=None,
        auto_plugins=None,
        required_resource_keys=None,
    ):
        self.name = check.opt_str_param(name, 'name')
        self.description = check.opt_str_param(description, 'description')
        self.loader = check.opt_inst_param(loader, 'loader', DagsterTypeLoader)
        self.materializer = check.opt_inst_param(
            materializer, 'materializer', DagsterTypeMaterializer
        )
        self.serialization_strategy = check.opt_inst_param(
            serialization_strategy,
            'serialization_strategy',
            SerializationStrategy,
            PickleSerializationStrategy(),
        )
        self.required_resource_keys = check.opt_set_param(
            required_resource_keys, 'required_resource_keys',
        )
        auto_plugins = check.opt_list_param(auto_plugins, 'auto_plugins', of_type=type)
        check.param_invariant(
            all(
                issubclass(auto_plugin_type, TypeStoragePlugin) for auto_plugin_type in auto_plugins
            ),
            'auto_plugins',
        )
        self.auto_plugins = auto_plugins

    def __call__(self, fn):
        check.callable_param(fn, 'fn')

        if not self.name:
            self.name = fn.__name__

        validate_type_check_fn(fn, self.name)

        dagster_type_def = DagsterType(
            type_check_fn=fn,
            key=None,
            name=self.name,
            is_builtin=False,
            description=self.description,
            loader=self.loader,
            materializer=self.materializer,
            serialization_strategy=self.serialization_strategy,
            auto_plugins=self.auto_plugins,
            required_resource_keys=self.required_resource_keys,
            kind=DagsterTypeKind.REGULAR,
        )
        update_wrapper(dagster_type_def, fn)
        return dagster_type_def


def dagster_type(
    name=None,
    description=None,
    loader=None,
    materializer=None,
    serialization_strategy=None,
    auto_plugins=None,
    required_resource_keys=None,
):
    '''Create a Dagster type using the decorated function as the type check.

    This shortcut simplifies the core :class:`DagsterType` API by exploding arguments into
    kwargs of the decorated compute function and omitting additional parameters when they are not
    needed.

    The decorated function will be used as the type check function. 

    Args:
        name (str): Name of the Dagster type. Must be unique within the repository.
        description (Optional[str]): A markdown-formatted string, displayed in tooling.
        loader (Optional[DagsterTypeLoader]): An instance of a class that inherits from
            :py:class:`~dagster.DagsterTypeLoader` and can map config data to a value of this type.
            Specify this argument if you will need to shim values of this type using the config
            machinery. As a rule, you should use the
            :py:func:`@dagster_type_loader <dagster.dagster_type_loader>` decorator to construct
            these arguments.
        materializer (Optional[DagsterTypeMaterializer]): An instance of a class that inherits
            from :py:class:`~dagster.DagsterTypeMaterializer` and can persist values of this type.
            As a rule, you should use the
            :py:func:`@dagster_type_materializer <dagster.dagster_type_materializer>`
            decorator to construct these arguments.
        serialization_strategy (Optional[SerializationStrategy]): An instance of a class that
            inherits from :py:class:`~dagster.SerializationStrategy`. The default strategy for
            serializing this value when automatically persisting it between execution steps. You
            should set this value if the ordinary serialization machinery (e.g., pickle) will not
            be adequate for this type.
        auto_plugins (Optional[List[Type[TypeStoragePlugin]]]): If types must be serialized
            differently depending on the storage being used for intermediates, they should specify
            this argument. In these cases the serialization_strategy argument is not sufficient
            because serialization requires specialized API calls, e.g. to call an S3 API directly
            instead of using a generic file object. See ``dagster_pyspark.DataFrame`` for an
            example.
        required_resource_keys (Optional[Set[str]]): Resource keys required by the
            ``type_check_fn``.


    Examples:

        .. code-block:: python

            @dagster_type(name='IntGt3')
            def int_gt_3(value):
                return isinstance(value, int) and value > 3
    '''
    # This case is for when decorator is used bare, without arguments. e.g. @dagster_type versus @dagster_type()
    if callable(name):
        check.invariant(description is None)
        check.invariant(loader is None)
        check.invariant(materializer is None)
        check.invariant(serialization_strategy is None)
        check.invariant(auto_plugins is None)
        check.invariant(required_resource_keys is None)

        return _DagsterType()(name)

    return _DagsterType(
        name=name,
        description=description,
        loader=loader,
        materializer=materializer,
        serialization_strategy=serialization_strategy,
        auto_plugins=auto_plugins,
        required_resource_keys=required_resource_keys,
    )
