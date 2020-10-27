import pytest
from dagster import lambda_solid


def test_lambda_solid_warning():
    with pytest.warns(
        UserWarning, match="Importing @lambda_solid from the top level in dagster is deprecated"
    ):

        @lambda_solid
        def _produce_string():
            return "foo"
