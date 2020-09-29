# start_a3186e28029e11eb9647acde48001122
from dagster import PresetDefinition, pipeline, repository, solid


@solid
def get_tag(context):
    owner = context.pipeline_run.tags.get("owner")
    context.log.info("owner is {}".format(owner))
    return owner


@pipeline(
    tags={"owner": "ml_team", "source": "pipeline"},
    preset_defs=[PresetDefinition("tag_preset", tags={"oncall": "ml_team", "source": "preset"})],
)
def tags_pipeline():
    get_tag()


@repository
def pipeline_tags_example():
    return [tags_pipeline]


# end_a3186e28029e11eb9647acde48001122
