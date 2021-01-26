from dagster_graphql.schema import types


def test_schema_naming_invariant():  # pylint: disable=protected-access
    violations = []
    for type_ in types():
        if type_.__name__ != f"Graphene{type_._meta.name}":
            violations.append((type_.__name__, f"Graphene{type_._meta.name}"))
    assert not violations, violations
