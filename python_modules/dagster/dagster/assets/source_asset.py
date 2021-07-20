from typing import Any, NamedTuple, Optional


class SourceAsset(NamedTuple):
    name: str
    namespace: Optional[str] = None
    metadata: Optional[Any] = None
