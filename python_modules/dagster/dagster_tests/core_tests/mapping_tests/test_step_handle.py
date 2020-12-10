from dagster.core.execution.plan.handle import DynamicStepHandle, StepHandle, UnresolvedStepHandle


def test_step_handles():
    plain = StepHandle.from_key("foo")
    assert isinstance(plain, StepHandle)
    unresolved = StepHandle.from_key("foo[?]")
    assert isinstance(unresolved, UnresolvedStepHandle)
    resolved = StepHandle.from_key("foo[bar]")
    assert isinstance(resolved, DynamicStepHandle)

    assert unresolved.resolve("bar") == resolved

    assert resolved.unresolved_form == unresolved
