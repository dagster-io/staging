from dagster.utils.test import assert_documented_exports

# This is the current list of undocumented exports.
# These should be split up to be documented.
# NOTE: No new entries are allowed to be added here.
WHITELIST = {
    "RetryRequested",
    "ScalarUnion",
    "DefaultRunLauncher",
    "build_intermediate_storage_from_object_store",
    "SolidExecutionContext",
    "DagsterInstance",
    "SerializationStrategy",
    "Materialization",
    "local_file_manager",
    "SystemStorageData",
    "PipelineRun",
}


def test_documentation():
    import dagster

    assert_documented_exports("dagster", dagster, whitelist=WHITELIST)
