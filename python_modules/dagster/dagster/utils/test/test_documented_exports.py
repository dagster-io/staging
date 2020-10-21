import json
import os
import subprocess

import six

from dagster import check


def git_repo_root():
    return six.ensure_str(subprocess.check_output(["git", "rev-parse", "--show-toplevel"]).strip())


def assert_documented_exports(module_name, module, whitelist=None):
    whitelist = check.opt_set_param(whitelist, "whitelist")

    # If this test is failing, but you have recently updated the documentation with the missing
    # export, run `make updateindex` from the docs directory
    all_exports = module.__all__
    path_to_export_index = os.path.join(git_repo_root(), "docs/next/src/data/exportindex.json")
    with open(path_to_export_index, "r") as f:
        export_index = json.load(f)
        documented_exports = set(export_index[module_name])
        undocumented_exports = set(all_exports).difference(documented_exports).difference(whitelist)
        assert len(undocumented_exports) == 0, undocumented_exports
