from typing import Dict, List

import dbt.tracking
from dagster import (
    CompositeSolidDefinition,
    DependencyDefinition,
    InputDefinition,
    Nothing,
    OutputDefinition,
    SolidDefinition,
)
from dagster_dbt.cli.utils import execute_cli
from dbt.main import parse_args


def graph_from_dbt_project(
    name: str, project_dir: str, profiles_dir: str
) -> CompositeSolidDefinition:
    """Generates a graph with a solid for each model in the given dbt project."""
    node_graph = node_graph_from_project(project_dir, profiles_dir)
    solids = [
        solid_from_dbt_model(node_name, node_dep_names, project_dir, profiles_dir)
        for node_name, node_dep_names in node_graph.items()
    ]

    deps = {
        model_name_to_def_name(node_name): {
            model_name_to_def_name(upstream_model_name): DependencyDefinition(
                model_name_to_def_name(upstream_model_name), "result"
            )
            for upstream_model_name in node_dep_names
        }
        for node_name, node_dep_names in node_graph.items()
    }

    return CompositeSolidDefinition(name=name, solid_defs=solids, dependencies=deps)


def node_graph_from_project(project_dir: str, profiles_dir: str) -> Dict[str, List[str]]:
    """
    Returns:
        Model names to model dep names
    """
    dbt.tracking.do_not_track()
    parsed_args = parse_args(["run", "--project-dir", project_dir, "--profiles-dir", profiles_dir])
    task = parsed_args.cls.from_args(args=parsed_args)
    task.load_manifest()
    return {node_name: node.depends_on_nodes for node_name, node in task.manifest.nodes.items()}


def solid_from_dbt_model(
    model_name: str, model_dep_names: List[str], project_dir: str, profiles_dir: str
) -> SolidDefinition:
    def compute_fn(context, _inputs):
        execute_cli(
            "dbt",
            command=("run",),
            flags_dict={
                "models": model_name,
                "project-dir": project_dir,
                "profiles-dir": profiles_dir,
            },
            log=context.log,
            warn_error=False,
            ignore_handled_error=False,
        )

    input_defs = [
        InputDefinition(model_name_to_def_name(name), dagster_type=Nothing)
        for name in model_dep_names
    ]

    return SolidDefinition(
        name=model_name_to_def_name(model_name),
        compute_fn=compute_fn,
        input_defs=input_defs,
        output_defs=[OutputDefinition(Nothing)],
    )


def model_name_to_def_name(model_name: str) -> str:
    return model_name.replace(".", "__")
