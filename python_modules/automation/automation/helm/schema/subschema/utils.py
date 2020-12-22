from enum import Enum
from typing import Dict, List


class SupportedKubernetes(str, Enum):
    V1_15 = "1.15.0"
    V1_16 = "1.16.0"


def create_definition_ref(definition: str, version: str = SupportedKubernetes.V1_15) -> str:
    return (
        f"https://kubernetesjsonschema.dev/v{version}/_definitions.json#/definitions/{definition}"
    )


def create_json_schema_conditionals(
    enum_type_to_config_name_mapping: Dict[Enum, str]
) -> List[dict]:
    return [
        {
            "if": {"properties": {"type": {"const": enum_type}},},
            "then": {"properties": {"config": {"required": [config_name]}}},
        }
        for (enum_type, config_name) in enum_type_to_config_name_mapping.items()
    ]
