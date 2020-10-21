from dagster.utils.test import assert_documented_exports

WHITELIST = {
    "ConstraintWithMetadataException",
    "all_unique_validator",
    "ColumnWithMetadataException",
    "categorical_column_validator_factory",
    "MultiConstraintWithMetadata",
    "MultiColumnConstraintWithMetadata",
    "non_null_validation",
    "StrictColumnsWithMetadata",
    "MultiAggregateConstraintWithMetadata",
    "ConstraintWithMetadata",
    "dtype_in_set_validation_factory",
    "nonnull",
    "create_structured_dataframe_type",
    "column_range_validation_factory",
}


def test_documentation():
    import dagster_pandas

    assert_documented_exports("dagster_pandas", dagster_pandas, whitelist=WHITELIST)
