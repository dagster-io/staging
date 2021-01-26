import graphene
from dagster import check
from dagster.config.config_type import ConfigTypeKind
from dagster.config.snap import get_recursive_type_keys
from dagster.core.snap import ConfigFieldSnap, ConfigSchemaSnapshot, ConfigTypeSnap

from .errors import PipelineNotFoundError, PythonError
from .util import non_null_list


def to_config_type(config_schema_snapshot, config_type_key):
    check.inst_param(config_schema_snapshot, "config_schema_snapshot", ConfigSchemaSnapshot)
    check.str_param(config_type_key, "config_type_key")

    config_type_snap = config_schema_snapshot.get_config_snap(config_type_key)
    kind = config_type_snap.kind

    if kind == ConfigTypeKind.ENUM:
        return EnumConfigType(config_schema_snapshot, config_type_snap)
    elif ConfigTypeKind.has_fields(kind):
        return CompositeConfigType(config_schema_snapshot, config_type_snap)
    elif kind == ConfigTypeKind.ARRAY:
        return ArrayConfigType(config_schema_snapshot, config_type_snap)
    elif kind == ConfigTypeKind.NONEABLE:
        return NullableConfigType(config_schema_snapshot, config_type_snap)
    elif kind == ConfigTypeKind.ANY or kind == ConfigTypeKind.SCALAR:
        return RegularConfigType(config_schema_snapshot, config_type_snap)
    elif kind == ConfigTypeKind.SCALAR_UNION:
        return ScalarUnionConfigType(config_schema_snapshot, config_type_snap)
    else:
        check.failed("Should never reach")


def _ctor_kwargs_for_snap(config_type_snap):
    return dict(
        key=config_type_snap.key,
        description=config_type_snap.description,
        is_selector=config_type_snap.kind == ConfigTypeKind.SELECTOR,
        type_param_keys=config_type_snap.type_param_keys or [],
    )


class ConfigType(graphene.Interface):
    key = graphene.NonNull(graphene.String)
    description = graphene.String()

    recursive_config_types = graphene.Field(
        non_null_list("ConfigType"),
        description="""
This is an odd and problematic field. It recursively goes down to
get all the types contained within a type. The case where it is horrible
are dictionaries and it recurses all the way down to the leaves. This means
that in a case where one is fetching all the types and then all the inner
types keys for those types, we are returning O(N^2) type keys, which
can cause awful performance for large schemas. When you have access
to *all* the types, you should instead only use the type_param_keys
field for closed generic types and manually navigate down the to
field types client-side.

Where it is useful is when you are fetching types independently and
want to be able to render them, but without fetching the entire schema.

We use this capability when rendering the sidebar.
    """,
    )
    type_param_keys = graphene.Field(
        non_null_list(graphene.String),
        description="""
This returns the keys for type parameters of any closed generic type,
(e.g. List, Optional). This should be used for reconstructing and
navigating the full schema client-side and not innerTypes.
    """,
    )
    is_selector = graphene.NonNull(graphene.Boolean)


class ConfigTypeMixin:
    def __init__(self, config_schema_snapshot, config_type_snap):
        self._config_type_snap = check.inst_param(
            config_type_snap, "config_type_snap", ConfigTypeSnap
        )
        self._config_schema_snapshot = check.inst_param(
            config_schema_snapshot, "config_schema_snapshot", ConfigSchemaSnapshot
        )
        super(ConfigTypeMixin, self).__init__(**_ctor_kwargs_for_snap(config_type_snap))

    def resolve_recursive_config_types(self, _graphene_info):
        return list(
            map(
                lambda key: to_config_type(self._config_schema_snapshot, key),
                get_recursive_type_keys(self._config_type_snap, self._config_schema_snapshot),
            )
        )


class RegularConfigType(ConfigTypeMixin, graphene.ObjectType):
    class Meta:
        interfaces = (ConfigType,)
        description = "Regular is an odd name in this context. It really means Scalar or Any."

    given_name = graphene.NonNull(graphene.String)

    def resolve_given_name(self, _):
        return self._config_type_snap.given_name


class WrappingConfigType(graphene.Interface):
    of_type = graphene.Field(graphene.NonNull(ConfigType))


class ArrayConfigType(ConfigTypeMixin, graphene.ObjectType):
    class Meta:
        interfaces = (ConfigType, WrappingConfigType)

    def resolve_of_type(self, _graphene_info):
        return to_config_type(self._config_schema_snapshot, self._config_type_snap.inner_type_key,)


class ScalarUnionConfigType(ConfigTypeMixin, graphene.ObjectType):
    class Meta:
        interfaces = (ConfigType,)

    scalar_type = graphene.NonNull(ConfigType)
    non_scalar_type = graphene.NonNull(ConfigType)

    scalar_type_key = graphene.NonNull(graphene.String)
    non_scalar_type_key = graphene.NonNull(graphene.String)

    def get_scalar_type_key(self):
        return self._config_type_snap.scalar_type_key

    def get_non_scalar_type_key(self):
        return self._config_type_snap.non_scalar_type_key

    def resolve_scalar_type_key(self, _):
        return self.get_scalar_type_key()

    def resolve_non_scalar_type_key(self, _):
        return self.get_non_scalar_type_key()

    def resolve_scalar_type(self, _):
        return to_config_type(self._config_schema_snapshot, self.get_scalar_type_key())

    def resolve_non_scalar_type(self, _):
        return to_config_type(self._config_schema_snapshot, self.get_non_scalar_type_key())


class NullableConfigType(ConfigTypeMixin, graphene.ObjectType):
    class Meta:
        interfaces = (ConfigType, WrappingConfigType)

    def resolve_of_type(self, _graphene_info):
        return to_config_type(self._config_schema_snapshot, self._config_type_snap.inner_type_key)


class EnumConfigValue(graphene.ObjectType):
    value = graphene.NonNull(graphene.String)
    description = graphene.String()


class EnumConfigType(ConfigTypeMixin, graphene.ObjectType):
    class Meta:
        interfaces = (ConfigType,)

    values = non_null_list(EnumConfigValue)
    given_name = graphene.NonNull(graphene.String)

    def resolve_values(self, _graphene_info):
        return [
            EnumConfigValue(value=ev.value, description=ev.description)
            for ev in self._config_type_snap.enum_values
        ]

    def resolve_given_name(self, _):
        return self._config_type_snap.given_name


class ConfigTypeField(graphene.ObjectType):
    name = graphene.NonNull(graphene.String)
    description = graphene.String()
    config_type = graphene.NonNull(ConfigType)
    config_type_key = graphene.NonNull(graphene.String)
    is_required = graphene.NonNull(graphene.Boolean)

    def resolve_config_type_key(self, _):
        return self._field_snap.type_key

    def __init__(self, config_schema_snapshot, field_snap):
        self._config_schema_snapshot = check.inst_param(
            config_schema_snapshot, "config_schema_snapshot", ConfigSchemaSnapshot
        )
        self._field_snap = check.inst_param(field_snap, "field_snap", ConfigFieldSnap)
        super(ConfigTypeField, self).__init__(
            name=field_snap.name,
            description=field_snap.description,
            is_required=field_snap.is_required,
        )

    def resolve_config_type(self, _graphene_info):
        return to_config_type(self._config_schema_snapshot, self._field_snap.type_key)


class CompositeConfigType(ConfigTypeMixin, graphene.ObjectType):
    class Meta:
        interfaces = (ConfigType,)

    fields = non_null_list(ConfigTypeField)

    def resolve_fields(self, _graphene_info):
        return sorted(
            [
                ConfigTypeField(
                    config_schema_snapshot=self._config_schema_snapshot, field_snap=field_snap,
                )
                for field_snap in self._config_type_snap.fields
            ],
            key=lambda field: field.name,
        )
