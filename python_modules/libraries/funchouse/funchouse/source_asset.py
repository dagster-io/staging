from typing import Any, NamedTuple, Optional, Tuple


class SourceAsset(NamedTuple):
    path: Tuple[str]
    metadata: Optional[Any] = None
