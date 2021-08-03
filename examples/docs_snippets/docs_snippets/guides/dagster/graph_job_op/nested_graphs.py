from dagster import graph, op


@op
def do_something():
    pass


@op
def do_something_else():
    pass


@graph
def do_two_things():
    do_something()
    do_something_else()


@op
def do_yet_more():
    pass


@graph
def do_it_all():
    do_two_things()
    do_yet_more()
