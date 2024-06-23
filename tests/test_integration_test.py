from testcontainers.core.image import DockerImage  # type: ignore
from testcontainers.core.container import DockerContainer  # type: ignore
from testcontainers.core.waiting_utils import wait_for_logs  # type: ignore
import os
import time
import pytest
from uc_wrapper import UCClient, Catalog, AlreadyExistsError, DoesNotExistError


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

            schemas = client.list_schemas(catalog=default_catalog)
            assert len(schemas) == 1
            assert schemas[0].name == default_schema
            assert schemas[0].catalog_name == default_catalog
            assert schemas[0].full_name == default_catalog + "." + default_schema
