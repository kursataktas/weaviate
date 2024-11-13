from typing import Union, List

import pytest
import weaviate
import weaviate.classes as wvc
from weaviate.rbac.models import RBAC, DatabaseAction, CollectionsAction
from _pytest.fixtures import SubRequest
from .conftest import _sanitize_role_name


def test_rbac_users(request: SubRequest):
    with weaviate.connect_to_local(
        port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("admin-key")
    ) as client:
        client.collections.delete(["target", "source"])
        # create two collections with some objects to test refs
        target = client.collections.create(name="target")
        source = client.collections.create(
            name="source",
            references=[wvc.config.ReferenceProperty(name="ref", target_collection=target.name)],
        )
        uuid_target1 = target.data.insert({})
        uuid_target2 = target.data.insert({})
        uuid_source = source.data.insert(properties={}, references={"ref": uuid_target1})
        role_name = _sanitize_role_name(request.node.name)
        client.roles.delete(role_name)

        # read+update for both
        with weaviate.connect_to_local(
            port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("no-rights-key")
        ) as client_no_rights:
            both_write = client.roles.create(
                name=role_name,
                permissions=RBAC.permissions.collection(
                    target.name, CollectionsAction.UPDATE_COLLECTIONS
                )
                + RBAC.permissions.collection(target.name, CollectionsAction.READ_COLLECTIONS)
                + RBAC.permissions.collection(source.name, CollectionsAction.UPDATE_COLLECTIONS)
                + RBAC.permissions.collection(source.name, CollectionsAction.READ_COLLECTIONS),
            )
            client.roles.assign(user="no-rights-user", roles=both_write.name)

            source_no_rights = client_no_rights.collections.get(
                source.name
            )  # no network call => no RBAC check
            source_no_rights.data.reference_add(
                from_uuid=uuid_source,
                from_property="ref",
                to=uuid_target1,
            )

            source_no_rights.data.reference_replace(
                from_uuid=uuid_source,
                from_property="ref",
                to=uuid_target2,
            )

            source_no_rights.data.reference_delete(
                from_uuid=uuid_source,
                from_property="ref",
                to=uuid_target2,
            )

            client.roles.revoke(user="no-rights-user", roles=both_write.name)
            client.roles.delete(both_write.name)

        # only read+update for one of them
        for col in [source.name]:
            with weaviate.connect_to_local(
                port=8081, grpc_port=50052, auth_credentials=wvc.init.Auth.api_key("no-rights-key")
            ) as client_no_rights:
                role = client.roles.create(
                    name=role_name,
                    permissions=RBAC.permissions.collection(
                        col, CollectionsAction.UPDATE_COLLECTIONS
                    )
                    + RBAC.permissions.collection(col, CollectionsAction.READ_COLLECTIONS),
                )
                client.roles.assign(user="no-rights-user", roles=role.name)

                source_no_rights = client_no_rights.collections.get(
                    source.name
                )  # no network call => no RBAC check

                with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
                    source_no_rights.data.reference_add(
                        from_uuid=uuid_source,
                        from_property="ref",
                        to=uuid_target1,
                    )
                assert e.value.status_code == 403

                with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
                    source_no_rights.data.reference_replace(
                        from_uuid=uuid_source,
                        from_property="ref",
                        to=uuid_target2,
                    )
                assert e.value.status_code == 403

                with pytest.raises(weaviate.exceptions.UnexpectedStatusCodeException) as e:
                    source_no_rights.data.reference_delete(
                        from_uuid=uuid_source,
                        from_property="ref",
                        to=uuid_target1,
                    )
                assert e.value.status_code == 403

                client.roles.revoke(user="no-rights-user", roles=role.name)
                client.roles.delete(role.name)
