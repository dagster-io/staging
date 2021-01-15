from dagster import execute_pipeline
from memoized_development.repo import my_pipeline


def test_memoized_development():
    result = execute_pipeline(
        my_pipeline,
        run_config={
            "solids": {
                "emit_dog": {"config": {"dog_breed": "poodle"}},
                "emit_tree": {"config": {"tree_species": "weeping_willow"}},
            }
        },
        mode="in_memory",
    )
    assert result.success
