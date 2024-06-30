from testcontainers.core.image import DockerImage  # type: ignore
from testcontainers.core.container import DockerContainer  # type: ignore
from testcontainers.core.waiting_utils import wait_for_logs  # type: ignore
import os
import time
import requests
import pytest
from collections.abc import Generator
from uc_wrapper import UCClient, Catalog, AlreadyExistsError, DoesNotExistError, Schema


USE_EXISTING_IMAGE_ENV_VAR = "UC_TEST_USE_IMAGE"
IMAGE_NAME_ENV_VAR = "UC_TEST_IMAGE_TAG"


@pytest.fixture(scope="module")
def image() -> Generator[str, None, None]:
    env_var = os.getenv(USE_EXISTING_IMAGE_ENV_VAR)
    if env_var is not None and env_var.upper() == "TRUE":
        image_name = os.getenv(IMAGE_NAME_ENV_VAR)
        assert image_name is not None
        yield image_name
        return
    path = os.path.join(*os.path.split(__file__)[:-1])  # path to the current file
    with DockerImage(path=path, tag="uc-catalog-test-image:latest") as img:
        yield str(img)


@pytest.fixture
def client(image: str) -> Generator[UCClient, None, None]:
    with DockerContainer(image).with_exposed_ports(8080) as container:
        _ = wait_for_logs(
            container,
            "###################################################################",
            120.0,
        )

        url = container.get_container_host_ip()
        port = container.get_exposed_port(8080)
        uc_url = "http://" + url + ":" + port

        # Unity Catalog does not log anything after it has started listening to its port
        # and it is not immediately ready after logging its startup message so we might
        # need to wait for it to be ready.
        counter = 0
        while counter <= 5:
            try:
                requests.get(uc_url)
                break
            except:
                if counter == 5:
                    raise Exception(
                        "Unity Catalog failed to be responsive within 5 seconds after logging startup message."
                    )
                time.sleep(1.0)
                counter += 1

        yield UCClient(uc_url=uc_url)


def test_catalogs_endpoint_intergration(client: UCClient):
    assert client.health_check()

    cat_name = "asdasdasdasfdsadgsa"
    cat_comment = "asd"
    catalog = Catalog(
        name=cat_name,
        comment=cat_comment,
    )

    cat_name_update = "asdgnölsavnsaödn"
    catalog_update = Catalog(name=cat_name_update)

    # At start, there is only the default catalog; verify this

    assert len(client.list_catalogs()) == 1

    cat = client.get_catalog("unity")
    assert cat is not None
    assert cat.name == "unity"

    with pytest.raises(DoesNotExistError):
        client.get_catalog(cat_name)

    # Then we create a new catalog; verify it was added and that we cannot overwrite it

    cat = client.create_catalog(catalog)
    assert cat.name == cat_name
    assert cat.comment == cat_comment
    assert cat.id is not None

    assert len(client.list_catalogs()) == 2

    with pytest.raises(AlreadyExistsError):
        client.create_catalog(catalog)

    with pytest.raises(DoesNotExistError):
        client.update_catalog("xyz_this_cat_does_not_exist", catalog_update)

    # Update the catalog we added; verify it was updated and the old catalog does not exist

    cat = client.update_catalog(cat_name, catalog_update)
    assert cat.name == cat_name_update
    assert cat.comment is None

    assert len(client.list_catalogs()) == 2

    with pytest.raises(DoesNotExistError):
        client.get_catalog(cat_name)
    assert not client.delete_catalog(cat_name, False)
    assert len(client.list_catalogs()) == 2

    # Finally we delete the updated catalog; verify it actually gets deleted and we cannot "re-delete" it

    assert client.delete_catalog(cat_name_update, False)
    assert len(client.list_catalogs()) == 1
    assert not client.delete_catalog(cat_name_update, False)
    assert len(client.list_catalogs()) == 1


def test_schemas_endpoint_intergration(client: UCClient):
    assert client.health_check()

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

    # Initially, there is only the default schema; verify this

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
        client.get_schema(catalog=default_catalog + "sfdgsagsd", schema=default_schema)

    with pytest.raises(DoesNotExistError):
        client.delete_schema(catalog=default_catalog, schema=new_schema_name)

    # Then we create a new schema; verify it was added and we cannot overwrite it

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

    # Update the schema; verify the schema was updated and the old schema does not exist

    schema = client.update_schema(
        catalog=default_catalog,
        schema_name=new_schema_name,
        new_schema=schema_update,
    )
    assert schema.full_name == default_catalog + "." + schema_name_update
    # Default comment set to "" since UC does not clear comment if the new comment is null
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

    # Finally we delete the updated schema; verify it got deleted and we cannot "re-delete" it

    client.delete_schema(catalog=default_catalog, schema=schema_name_update)

    schemas = client.list_schemas(catalog=default_catalog)
    assert len(schemas) == 1

    with pytest.raises(DoesNotExistError):
        client.delete_schema(catalog=default_catalog, schema=new_schema_name)
