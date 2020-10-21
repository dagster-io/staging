import json
import os
import subprocess

import six

import dagster


def git_repo_root():
    return six.ensure_str(subprocess.check_output(["git", "rev-parse", "--show-toplevel"]).strip())


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
    # If this test is failing, but you have recently updated the documentation with the missing
    # export, run `make searchindex` from the docs directory
    all_exports = dagster.__all__
    path_to_search_index = os.path.join(git_repo_root(), "docs/next/src/data/searchindex.json")
    with open(path_to_search_index, "r") as f:
        search_index = json.load(f)
        documented_exports = set(search_index["objects"]["dagster"].keys())
        undocumented_exports = set(all_exports).difference(documented_exports).difference(WHITELIST)
        assert len(undocumented_exports) == 0, undocumented_exports
