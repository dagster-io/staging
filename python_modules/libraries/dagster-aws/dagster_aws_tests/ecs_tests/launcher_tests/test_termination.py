def test_termination(instance, workspace, run):

    assert not instance.can_terminate_run(run.run_id)

    instance.launch_run(run.run_id, workspace)

    assert instance.can_terminate_run(run.run_id)
    assert instance.terminate_run(run.run_id)
    assert not instance.can_terminate_run(run.run_id)
    assert not instance.terminate_run(run.run_id)
