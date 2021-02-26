import json
import os
import subprocess
from dataclasses import dataclass
from tempfile import NamedTemporaryFile
from typing import List

import yaml
from schema.schema.values import HelmValues


def git_repo_root():
    return subprocess.check_output(["git", "rev-parse", "--show-toplevel"]).decode("utf-8").strip()


@dataclass
class HelmTemplate:
    output: str
    name: str = "RELEASE-NAME"

    def render(self, values: HelmValues) -> List[dict]:
        with NamedTemporaryFile() as tmp_file:
            content = yaml.dump(json.loads(values.json(exclude_none=True)))
            tmp_file.write(content.encode())
            tmp_file.flush()

            command = [
                "helm",
                "template",
                self.name,
                os.path.join(git_repo_root(), "helm", "dagster"),
                "--dependency-update",
                *['--values', tmp_file.name],
                *["--show-only", self.output],
            ]

            templates = subprocess.check_output(command)
            k8s_objects = [k8s_object for k8s_object in yaml.full_load_all(templates) if k8s_object]

            return k8s_objects
