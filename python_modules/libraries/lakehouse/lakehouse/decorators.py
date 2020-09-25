from collections import OrderedDict

from dagster import check
from dagster.seven import funcsigs

from .asset import Asset
from .computation import AssetDependency, Computation
from .table import Table


def computed_asset(
    storage_key="default_storage", path=None, input_assets=None, version=None, partitions=None
):
    """Create an Asset with its computation built from the decorated function.

    The type annotations on the arguments and return value of the decorated function are use to
    determine which TypeStoragePolicy will be used to load and save its outputs and inputs.

    Args:
        storage_key (str): The key of the storage used to persist the asset. Defaults to
            "default_storage".
        path (Optional[Tuple[str, ...]]): The path of the asset within the storage_key. If not
            given, the name of the decorated function is used.
        input_assets (Optional[Union[List[Asset], Dict[str, Asset]]]): The assets that this asset
            depends on, mapped to the arguments of the decorated function.  If a dictionary is
            passed, the keys should be the same as the names of the decorated function's arguments.
            If a list is passed, the first asset in the list is mapped to the first argument of the
            decorated function, and so on.
        version (Optional[str]): The version of the asset's computation.  Two assets should have
            the same version if and only if their computations deterministically produce the same
            outputs when provided the same inputs.

    Examples:

        .. code-block:: python

            @computed_asset(storage_key='filesystem', input_assets=[orders_asset])
            def asia_orders_asset(orders: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
                return orders.filter(orders['continent'] == 'asia')

            @computed_asset(storage_key='filesystem', input_assets={'orders': orders_asset})
            def asia_orders_asset(orders: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
                return orders.filter(orders['continent'] == 'asia')

            @computed_asset(storage_key='filesystem', input_assets=[orders_asset, users_asset])
            def user_orders_asset(
                orders: pyspark.sql.DataFrame, users: pyspark.sql.DataFrame
            ) -> pyspark.sql.DataFrame:
                return orders.join(users, 'user_id')

            @computed_asset(storage_key='filesystem', input_assets=[orders_asset])
            def orders_count_asset(orders: pyspark.sql.DataFrame) -> int:
                return orders.count()

            @computed_asset(storage_key='filesystem')
            def one_asset() -> int:
                return 1

    """

    def _computed_asset(fn):
        _path = path or (fn.__name__,)
        _input_assets = input_assets or []
        num_base_args = 1 if partitions else 0
        kwarg_deps = _deps_by_arg_name(_input_assets, fn, num_base_args)

        return Asset(
            storage_key=storage_key,
            path=_path,
            computation=Computation(
                compute_fn=fn,
                deps=kwarg_deps,
                output_in_memory_type=_infer_output_type(fn),
                version=version,
            ),
            partitions=partitions,
        )

    return _computed_asset


def computed_table(
    storage_key="default_storage",
    path=None,
    input_assets=None,
    columns=None,
    version=None,
    partitions=None,
):
    """Create a Table with its computation built from the decorated function.

    The type annotations on the arguments and return value of the decorated function are use to
    determine which TypeStoragePolicy will be used to load and save its outputs and inputs.

    Args:
        storage_key (str): The key of the storage used to persist the asset. Defaults to
            "default_storage".
        path (Optional[Tuple[str, ...]]): The path of the asset within the storage_key. If not
            given, the name of the decorated function is used.
        input_assets (Optional[Union[List[Asset], Dict[str, Asset]]]): The assets that this asset
            depends on, mapped to the arguments of the decorated function.  If a dictionary is
            passed, the keys should be the same as the names of the decorated function's arguments.
            If a list is passed, the first asset in the list is mapped to the first argument of the
            decorated function, and so on.
        columns (Optional[List[Column]]): The table's columns.
        version (Optional[str]): The version of the table's computation. Two tables should have
            the same version if and only if their computations deterministically produce the same
            outputs when provided the same inputs.
    """

    def _computed_table(fn):
        _path = path or (fn.__name__,)
        _input_assets = input_assets or []
        num_base_args = 1 if partitions else 0
        kwarg_deps = _deps_by_arg_name(_input_assets, fn, num_base_args)

        return Table(
            storage_key=storage_key,
            path=_path,
            computation=Computation(
                compute_fn=fn,
                deps=kwarg_deps,
                output_in_memory_type=_infer_output_type(fn),
                version=version,
            ),
            columns=columns,
            partitions=partitions,
        )

    return _computed_table


def _deps_by_arg_name(input_assets, fn, num_base_args):
    """
    Args:
        input_assets (Optional[Union[List[Asset], Dict[str, Asset]]])
        fn (Callable)

    Returns (Dict[str, AssetDependency])
    """
    kwarg_types = _infer_kwarg_types(fn)
    input_kwarg_types = kwarg_types[num_base_args:]
    if isinstance(input_assets, list):
        check.invariant(
            len(kwarg_types) == len(input_assets) + num_base_args,
            f'For {fn.__name__}, input_assets length "{len(input_assets)}"" must match number of '
            f'keyword args "{len(kwarg_types)}" plus number of base args "{num_base_args}".',
        )
        return {
            kwarg: AssetDependency(input_asset, annotated_type)
            for (kwarg, annotated_type), input_asset in zip(input_kwarg_types, input_assets)
        }
    elif isinstance(input_assets, dict):
        input_kwarg_names = [kwarg for kwarg, _ in input_kwarg_types]
        input_asset_keys = list(input_assets.keys())
        check.invariant(
            input_kwarg_names == input_asset_keys,
            f"input_assets keys {input_asset_keys} must match keyword args {input_kwarg_names}",
        )
        return {
            kwarg: AssetDependency(input_assets[kwarg], annotated_type)
            for kwarg, annotated_type in input_kwarg_types
        }
    else:
        check.failed("input_assets must be a list or a dict")


def _infer_kwarg_types(fn):
    signature = funcsigs.signature(fn)
    params = signature.parameters.values()
    return [(param.name, param.annotation) for param in params]


def _infer_output_type(fn):
    signature = funcsigs.signature(fn)
    return signature.return_annotation
