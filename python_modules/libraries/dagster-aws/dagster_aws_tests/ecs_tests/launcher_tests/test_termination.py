def test_termination(build_instance, pipeline, external_pipeline):
    instance = build_instance()

    run = instance.create_run_for_pipeline(pipeline)

    assert not instance.run_launcher.can_terminate(run.run_id)

    instance.launch_run(run.run_id, external_pipeline)

    assert instance.run_launcher.can_terminate(run.run_id)
    assert instance.run_launcher.terminate(run.run_id)
    assert not instance.run_launcher.can_terminate(run.run_id)
    assert not instance.run_launcher.terminate(run.run_id)
