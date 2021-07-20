from typing import Any, NamedTuple, Optional

from dagster.core.definitions.events import AssetKey


class SourceAsset(NamedTuple):
    key: AssetKey
    metadata: Optional[Any] = None
