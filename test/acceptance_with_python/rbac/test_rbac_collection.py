import pytest
import weaviate
import weaviate.classes as wvc
from weaviate.rbac.models import RBAC
from _pytest.fixtures import SubRequest
from .conftest import _sanitize_role_name


def test_rbac_collection_create(request: SubRequest, cleanup_role):

    with weaviate.connect_to_local(
        port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("admin-key")
    ) as client:
        name_collection = _sanitize_role_name(request.node.name) + "col"
        client.collections.delete(name_collection)
        name_role = cleanup_role  # This role will be cleaned up even if test fails

        # with create rights
        with weaviate.connect_to_local(
            port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("custom-key")
        ) as client_with_rights:
            client_with_rights.roles.create(
                name=name_role,
                permissions=RBAC.permissions.collections.create(),
            )
            client.roles.assign(user="custom-user", roles=name_role)

            col = client_with_rights.collections.create(
                name=name_collection,
                properties=[wvc.config.Property(name="prop", data_type=wvc.config.DataType.TEXT)],
            )
            assert col is not None

            client.roles.revoke(user="custom-user", roles=name_role)
            client.roles.delete(name_role)

        # without create rights
        with weaviate.connect_to_local(
            port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("custom-key")
        ) as client_no_rights:
            with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
                client_no_rights.collections.create(
                    name=name_collection,
                    properties=[
                        wvc.config.Property(name="prop", data_type=wvc.config.DataType.TEXT)
                    ],
                )
            assert e.value.status_code == 403
            assert "forbidden" in e.value.args[0]

        client.collections.delete(name_collection)


def test_rbac_collection_read(request: SubRequest, cleanup_role):

    with weaviate.connect_to_local(
        port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("admin-key")
    ) as client:
        name_collection = _sanitize_role_name(request.node.name) + "col"
        client.collections.delete(name_collection)
        name_role = cleanup_role  # This role will be cleaned up even if test fails

        col = client.collections.create(
            name=name_collection,
            properties=[wvc.config.Property(name="prop", data_type=wvc.config.DataType.TEXT)],
        )

        # with read rights
        with weaviate.connect_to_local(
            port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("custom-key")
        ) as client_with_rights:
            client.roles.create(
                name=name_role,
                permissions=RBAC.permissions.collections.read(collection=col.name),
            )
            client.roles.assign(user="custom-user", roles=name_role)

            # Test collection exists
            assert client_with_rights.collections.exists(col.name)

            # Test get collection
            col_with_rights = client_with_rights.collections.get(col.name)
            assert col_with_rights is not None

            # It shouldn't be possible to list all collections
            with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
                client_with_rights.collections.list_all()
            assert e.value.status_code == 403
            assert "forbidden" in e.value.args[0]

            client.roles.revoke(user="custom-user", roles=name_role)
            client.roles.delete(name_role)

        # without read rights
        with weaviate.connect_to_local(
            port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("custom-key")
        ) as client_no_rights:
            with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
                client_no_rights.collections.exists(col.name)
            assert e.value.status_code == 403
            assert "forbidden" in e.value.args[0]

            with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
                client_no_rights.collections.get(col.name)
            assert e.value.status_code == 403
            assert "forbidden" in e.value.args[0]

            with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
                client_no_rights.collections.list_all()
            assert e.value.status_code == 403
            assert "forbidden" in e.value.args[0]

        client.collections.delete(name_collection)
