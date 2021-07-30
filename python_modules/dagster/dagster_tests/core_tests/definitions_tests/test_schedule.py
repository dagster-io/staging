from dagster import ScheduleDefinition, graph


def test_default_name():
    schedule = ScheduleDefinition(pipeline_name="my_pipeline", cron_schedule="0 0 * * *")
    assert schedule.name == "my_pipeline_schedule"


def test_default_name_graph():
    @graph
    def my_graph():
        pass

    schedule = ScheduleDefinition(job=my_graph, cron_schedule="0 0 * * *")
    assert schedule.name == "my_graph_schedule"


def test_default_name_job():
    @graph
    def my_graph():
        pass

    schedule = ScheduleDefinition(job=my_graph.to_job(name="my_job"), cron_schedule="0 0 * * *")
    assert schedule.name == "my_job_schedule"


def test_str_job():
    from dagster import schedule, repository

    @graph
    def foo():
        pass

    @schedule(job="foo", cron_schedule="0 0 * * *")
    def my_schedule():
        pass

    @repository
    def _repo():
        return [foo, my_schedule]
