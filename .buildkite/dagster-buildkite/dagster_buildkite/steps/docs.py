# pylint: disable=anomalous-backslash-in-string

from typing import List

from ..defines import SupportedPython
from ..step_builder import StepBuilder


def docs_steps() -> List[dict]:
    return [
        # TODO: Yuhan to fix
        # StepBuilder("docs sphinx build")
        # .run(
        #     "pip install -e python_modules/automation",
        #     "pip install -r docs-requirements.txt -qqq",
        #     "pushd docs; make build",
        #     "git diff --exit-code",
        # )
        # .on_integration_image(SupportedPython.V3_7)
        # .build(),
        StepBuilder("docs code snapshots")
        .run(
            "pushd docs; make snapshot",
            "git diff --exit-code",
            "[[ \$? = 1 ]] && echo -e '--- Snapshot failed' && echo 'This test is failing because you have either "
            "(1) Updated the code used in a literalinclude or "
            "(2) Updated a literalinclude snapshot instead of updating the reference. "
            "Run make snapshots in the /docs directory to fix this error.'",
        )
        .on_integration_image(SupportedPython.V3_7)
        .build(),
    ]
