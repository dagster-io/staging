from dagster import daily_partitioned_config, graph, op, repository, schedule_from_partitions


@op(config_schema={"date": str})
def my_op():
    ...


@graph
def my_graph():
    my_op()


@daily_partitioned_config(start_date="2020-01-01")
def my_daily_config(start, _end):
    return {"solids": {"my_op": {"config": {"date": str(start)}}}}


my_schedule = schedule_from_partitions(my_graph.to_job(config=my_daily_config))


@repository
def my_repo():
    return [my_schedule]
