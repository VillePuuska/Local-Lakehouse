from testcontainers.core.image import DockerImage  # type: ignore
from testcontainers.core.container import DockerContainer  # type: ignore
from testcontainers.core.waiting_utils import wait_for_logs  # type: ignore
import os
import time
import pytest
from uc_wrapper import UCClient, Catalog, AlreadyExistsError, DoesNotExistError, Schema


# For anyone wandering into here, don't do your intergration tests like this.
# I'm just too lazy to do this better for now.
def test_full_acceptance_test():
    path = os.path.join(*os.path.split(__file__)[:-1])  # path to the current file
    with DockerImage(path=path, tag="uc-catalog-test-image:latest") as image:
        with DockerContainer(str(image)).with_exposed_ports(8080) as container:
            _ = wait_for_logs(
                container,
                "###################################################################",
                120.0,
            )
            time.sleep(5.0)  # extra sleep to let Unity Catalog to start listening

            url = container.get_container_host_ip()
            port = container.get_exposed_port(8080)
            uc_url = "http://" + url + ":" + port

            client = UCClient(uc_url)

            assert client.health_check()

            #
            # catalogs endpoint tests
            #
            cat_name = "asdasdasdasfdsadgsa"
            cat_comment = "asd"
            catalog = Catalog(
                name=cat_name,
                comment=cat_comment,
            )

            cat_name_update = "asdgnölsavnsaödn"
            catalog_update = Catalog(name=cat_name_update)

            assert len(client.list_catalogs()) == 1

            cat = client.get_catalog("unity")
            assert cat is not None
            assert cat.name == "unity"

            with pytest.raises(DoesNotExistError):
                client.get_catalog(cat_name)

            cat = client.create_catalog(catalog)
            assert cat.name == cat_name
            assert cat.comment == cat_comment
            assert cat.id is not None

            assert len(client.list_catalogs()) == 2

            with pytest.raises(AlreadyExistsError):
                client.create_catalog(catalog)

            with pytest.raises(DoesNotExistError):
                client.update_catalog("xyz_this_cat_does_not_exist", catalog_update)

            cat = client.update_catalog(cat_name, catalog_update)
            assert cat.name == cat_name_update
            assert cat.comment is None

            assert len(client.list_catalogs()) == 2

            with pytest.raises(DoesNotExistError):
                client.get_catalog(cat_name)
            assert not client.delete_catalog(cat_name, False)
            assert len(client.list_catalogs()) == 2

            assert client.delete_catalog(cat_name_update, False)
            assert len(client.list_catalogs()) == 1
            assert not client.delete_catalog(cat_name_update, False)
            assert len(client.list_catalogs()) == 1

            #
            # schemas endpoint tests
            #
            default_catalog = "unity"
            default_schema = "default"

            new_schema_name = "asdasdasdasfdsadgsa"
            new_schema_comment = "asd"
            new_schema = Schema(
                name=new_schema_name,
                catalog_name=default_catalog,
                comment=new_schema_comment,
            )

            schema_name_update = "asdgnölsavnsaödn"
            schema_update = Schema(
                name=schema_name_update,
                catalog_name=default_catalog,
            )

            try:
                client.delete_schema(default_catalog, new_schema_name)
            except:
                pass
            try:
                client.delete_schema(default_catalog, schema_name_update)
            except:
                pass

            assert client.health_check()

            schemas = client.list_schemas(catalog=default_catalog)
            assert len(schemas) == 1
            assert schemas[0].name == default_schema
            assert schemas[0].catalog_name == default_catalog
            assert schemas[0].full_name == default_catalog + "." + default_schema

            schema = client.get_schema(catalog=default_catalog, schema=default_schema)
            assert schema.name == default_schema
            assert schema.catalog_name == default_catalog
            assert schema.created_at is not None
            assert schema.updated_at is None

            with pytest.raises(DoesNotExistError):
                client.get_schema(
                    catalog=default_catalog, schema=default_schema + "sadfasgsagasg"
                )

            with pytest.raises(DoesNotExistError):
                client.get_schema(
                    catalog=default_catalog + "sfdgsagsd", schema=default_schema
                )

            with pytest.raises(DoesNotExistError):
                client.delete_schema(catalog=default_catalog, schema=new_schema_name)

            schema = client.create_schema(schema=new_schema)
            assert schema.full_name == default_catalog + "." + new_schema_name
            assert schema.comment == new_schema_comment
            assert schema.created_at is not None
            assert schema.updated_at is None
            assert schema.schema_id is not None

            with pytest.raises(AlreadyExistsError):
                client.create_schema(schema=new_schema)

            schemas = client.list_schemas(catalog=default_catalog)
            assert len(schemas) == 2

            with pytest.raises(AlreadyExistsError):
                client.update_schema(
                    catalog=default_catalog,
                    schema_name=new_schema_name,
                    new_schema=new_schema,
                )

            with pytest.raises(DoesNotExistError):
                client.update_schema(
                    catalog=default_catalog,
                    schema_name=schema_name_update,
                    new_schema=schema_update,
                )

            schema = client.update_schema(
                catalog=default_catalog,
                schema_name=new_schema_name,
                new_schema=schema_update,
            )
            assert schema.full_name == default_catalog + "." + schema_name_update
            # Default comment set to "" since UC does not clear comment is new comment is null
            assert schema.comment == ""
            assert schema.updated_at is not None

            with pytest.raises(DoesNotExistError):
                client.get_schema(catalog=default_catalog, schema=new_schema_name)

            schemas = client.list_schemas(catalog=default_catalog)
            assert len(schemas) == 2

            with pytest.raises(DoesNotExistError):
                client.delete_schema(catalog=default_catalog, schema=new_schema_name)

            schemas = client.list_schemas(catalog=default_catalog)
            assert len(schemas) == 2

            client.delete_schema(catalog=default_catalog, schema=schema_name_update)

            schemas = client.list_schemas(catalog=default_catalog)
            assert len(schemas) == 1

            with pytest.raises(DoesNotExistError):
                client.delete_schema(catalog=default_catalog, schema=new_schema_name)
