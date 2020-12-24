from ..defines import SupportedPython
from ..module_build_spec import ModuleBuildSpec


def windows_steps():
    return ModuleBuildSpec(
        "python_modules/dagster",
        env_vars=["AWS_ACCOUNT_ID"],
        supported_pythons=[SupportedPython.V3_8],
        tox_env_suffixes=[
            # "-api_tests",
            "-cli_tests",
            # "-core_tests",
            # "-daemon_tests",
            # "-general_tests",
            # "-scheduler_tests",
        ],
        upload_coverage=False,
    ).get_tox_build_steps(for_windows=True)
