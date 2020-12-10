from dagster.core.execution.plan.handle import DynamicStepHandle, StepHandle, UnresolvedStepHandle


def test_step_handles():
    plain = StepHandle.from_key("foo")
    assert isinstance(plain, StepHandle)
    unresolved = StepHandle.from_key("foo[?]")
    assert isinstance(unresolved, UnresolvedStepHandle)
    mapped = StepHandle.from_key("foo[bar]")
    assert isinstance(mapped, DynamicStepHandle)
