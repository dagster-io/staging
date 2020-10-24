KUBERNETES_VERSION = "1.15.0"


def create_definition_ref(definition: str, version: str = KUBERNETES_VERSION) -> str:
    return (
        f"https://kubernetesjsonschema.dev/v{version}/_definitions.json#/definitions/{definition}"
    )
