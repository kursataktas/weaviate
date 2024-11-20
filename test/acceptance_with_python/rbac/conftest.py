import pytest
import weaviate
import weaviate.classes as wvc
from _pytest.fixtures import SubRequest


def _sanitize_role_name(name: str) -> str:
    return (
        name.replace("[", "")
        .replace("]", "")
        .replace("-", "")
        .replace(" ", "")
        .replace(".", "")
        .replace("{", "")
        .replace("}", "")
    )


@pytest.fixture
def cleanup_role(request: SubRequest):
    name_role = _sanitize_role_name(request.node.name) + "role"

    def _cleanup():
        with weaviate.connect_to_local(
            port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("admin-key")
        ) as client:
            client.roles.delete(name_role)

    request.addfinalizer(_cleanup)
    return name_role
