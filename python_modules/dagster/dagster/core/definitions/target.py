from typing import List, NamedTuple, Optional


class RepoRelativeTarget(NamedTuple):
    pipeline_name: str
    mode: str
    solid_selection: Optional[List[str]]
