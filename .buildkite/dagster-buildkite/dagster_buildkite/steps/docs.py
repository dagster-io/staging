# pylint: disable=anomalous-backslash-in-string

from typing import List

from ..defines import SupportedPython
from ..step_builder import StepBuilder


def docs_steps() -> List[dict]:
    return [
        #  If this test is failing because you may have either:
        #   (1) Updated the code that is referenced by a literal include code snippet in the documentation
        #   (2) Directly modified the inline snapshot of a literal include code snippet instead of updating
        #       the underlying code that the literal include is pointing to.
        # To fix this, run 'yarn snapshots' in the /docs/next directory to update the snapshots.
        # Be sure to check the diff to make sure the code snippets are as you expect them."
        StepBuilder("docs code snapshots")
        .run("pushd docs; make snapshot", "git diff --exit-code")
        .on_integration_image(SupportedPython.V3_7)
        .build(),
        # TODO: Yuhan to fix
        # StepBuilder("docs sphinx json build")
        # .run(
        #     "pip install -e python_modules/automation",
        #     "pip install -r docs-requirements.txt -qqq",
        #     "pushd docs; make build",
        #     "git diff --exit-code",
        # )
        # .on_integration_image(SupportedPython.V3_7)
        # .build(),
        StepBuilder("docs next build")
        .run(
            "pushd docs/next",
            "yarn",
            "yarn build",
        )
        .on_integration_image(SupportedPython.V3_7)
        .build(),
    ]
