from typing import List

from ..defines import SupportedPython
from ..step_builder import StepBuilder


def docs_steps() -> List[dict]:
    return [
        # If this test is failing because you may have either:
        #   (1) Updated the code that is referenced by a literalinclude in the documentation
        #   (2) Directly modified the inline snapshot of a literalinclude instead of updating
        #       the underlying code that the literalinclude is pointing to.
        # To fix this, run 'make snapshot' in the /docs directory to update the snapshots.
        # Be sure to check the diff to make sure the literalincludes are as you expect them."
        StepBuilder("docs code snapshots")
        .run("pushd docs; make docs_dev_install; make snapshot", "git diff --exit-code")
        .on_integration_image(SupportedPython.V3_7)
        .build(),
        # If this test is failing because either:
        #   (1) Broken links in either MDX files or in the navigation json.
        #       Run `yarn test` to debug locally.
        #   (2) The docs site couldn't build successfully.
        #       Run `yarn build` to debug locally.
        StepBuilder("docs next")
        .run(
            "pushd docs/next",
            "yarn",
            "yarn test",
            "yarn build",
        )
        .on_integration_image(SupportedPython.V3_7)
        .build(),
        # If this test is failing because your content/api data is out-of-sync.
        # Run `make build` in the /docs directory to build the api data.
        StepBuilder("docs sphinx json build")
        .run(
            "pushd docs; pip install -r docs-requirements.txt -qqq",
            "make build",
            "git diff --exit-code",
        )
        .on_integration_image(SupportedPython.V3_7)
        .build(),
    ]
