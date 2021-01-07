from typing import Dict, List, Optional


def trigger_step(
    pipeline: str, branches: Optional[List[str]] = None, async_step: bool = False
) -> Dict:
    """trigger_step: Trigger a build of another pipeline.
    """
    step = {"label": f":rocket: {pipeline}", "async": async_step}
    if branches:
        step["branches"] = " ".join(branches)

    return step
