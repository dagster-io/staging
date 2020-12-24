from ..step_builder import StepBuilder


def windows_steps():
    return [
        StepBuilder("dagster py38-windows-core_tests")
        .run(
            "cd python_modules/dagster",
            "pip install -r dev-requirements.txt",
            "tox -vv -e py38-windows-core_tests",
        )
        .on_windows_image()
        .with_timeout(30)
        .build()
    ]
