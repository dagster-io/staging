from dagster.utils.test import assert_documented_exports


def test_documentation():
    import dagster_gcp

    assert_documented_exports(
        "dagster_gcp", dagster_gcp,
    )
