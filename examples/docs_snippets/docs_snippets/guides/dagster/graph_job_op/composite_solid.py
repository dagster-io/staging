from dagster import composite_solid, pipeline, solid


@solid
def do_something():
    pass


@solid
def do_something_else():
    pass


@composite_solid
def do_two_things():
    do_something()
    do_something_else()


@solid
def do_yet_more():
    pass


@pipeline
def do_it_all():
    do_two_things()
    do_yet_more()
