import graphene
from dagster import check
from dagster.core.definitions.events import AssetKey as DagsterAssetKey
from dagster.core.events import StepMaterializationData
from dagster.core.events.log import EventRecord

from ..implementation.fetch_assets import get_asset_events, get_asset_run_ids
from ..implementation.fetch_runs import get_run_by_id
from .errors import Error, PythonError
from .util import non_null_list


class AssetKey(graphene.ObjectType):
    path = non_null_list(graphene.String)
