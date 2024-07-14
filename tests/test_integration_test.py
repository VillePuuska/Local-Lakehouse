from testcontainers.core.image import DockerImage  # type: ignore
from testcontainers.core.container import DockerContainer  # type: ignore
from testcontainers.core.waiting_utils import wait_for_logs  # type: ignore
import os
import tempfile
import time
import requests
import pytest
from collections.abc import Generator
from uc_wrapper import (
    UCClient,
    Catalog,
    Column,
    AlreadyExistsError,
    DoesNotExistError,
    Schema,
    Table,
    TableType,
    FileType,
    DataType,
)


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

    default_catalog = "unity"

    cat_name = "asdasdasdasfdsadgsa"
    cat_comment = "asd"
    catalog = Catalog(
        name=cat_name,
        comment=cat_comment,
    )

    cat_name_update = "asdgnlsavnsadn"
    cat_comment_update = "ayo"
    catalog_update = Catalog(name=cat_name_update)
    catalog_update2 = Catalog(name=cat_name_update, comment=cat_comment_update)

    # At start, there is only the default catalog; verify this

    assert len(client.list_catalogs()) == 1

    cat = client.get_catalog(default_catalog)
    assert cat is not None
    assert cat.name == default_catalog

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

    # Try to update the default catalog with the info of the catalog we created;
    # this should not go through

    with pytest.raises(AlreadyExistsError):
        client.update_catalog(default_catalog, catalog)

    # Update the catalog name; verify it was updated and the old catalog does not exist

    cat = client.update_catalog(cat_name, catalog_update)
    assert cat.name == cat_name_update
    assert cat.comment == cat_comment

    assert len(client.list_catalogs()) == 2

    with pytest.raises(DoesNotExistError):
        client.get_catalog(cat_name)
    with pytest.raises(DoesNotExistError):
        client.delete_catalog(cat_name, False)
    assert len(client.list_catalogs()) == 2

    # Update just the comment but not the name; this used to be impossible due to a UC bug

    cat = client.update_catalog(cat_name_update, catalog_update2)
    assert cat.name == cat_name_update
    assert cat.comment == cat_comment_update

    # Delete the updated catalog; verify it actually gets deleted and we cannot "re-delete" it

    assert client.delete_catalog(cat_name_update, False)
    assert len(client.list_catalogs()) == 1
    with pytest.raises(DoesNotExistError):
        client.delete_catalog(cat_name_update, False)
    assert len(client.list_catalogs()) == 1

    # Test that we cannot delete the default catalog that has a schema without specifying force=True

    assert not client.delete_catalog(default_catalog, False)
    assert client.delete_catalog(default_catalog, True)
    assert len(client.list_catalogs()) == 0


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

    schema_name_update = "asdgnlsavnsadn"
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

    # Test that we cannot delete the default schema that has tables without specifying force=True

    assert not client.delete_schema(default_catalog, default_schema, False)
    assert client.delete_schema(default_catalog, default_schema, True)
    assert len(client.list_schemas(catalog=default_catalog)) == 0


def test_tables_endpoint_integration(client: UCClient):
    assert client.health_check()

    default_catalog = "unity"
    default_schema = "default"

    default_external_table = Table(
        name="numbers",
        catalog_name=default_catalog,
        schema_name=default_schema,
        table_type=TableType.EXTERNAL,
        file_type=FileType.DELTA,
        columns=[
            Column(
                name="as_int",
                data_type=DataType.INT,
                position=0,
                nullable=False,
            ),
            Column(
                name="as_double",
                data_type=DataType.DOUBLE,
                position=1,
                nullable=False,
            ),
        ],
    )

    default_managed_table = Table(
        name="marksheet",
        catalog_name=default_catalog,
        schema_name=default_schema,
        table_type=TableType.MANAGED,
        file_type=FileType.DELTA,
        columns=[
            Column(
                name="id",
                data_type=DataType.INT,
                position=0,
                nullable=False,
            ),
            Column(
                name="name",
                data_type=DataType.STRING,
                position=1,
                nullable=False,
            ),
            Column(
                name="marks",
                data_type=DataType.INT,
                position=2,
                nullable=True,
            ),
        ],
    )

    tmpfilepath = tempfile.mkdtemp()

    new_external_table = Table(
        name="testasdadadassfgsdg",
        catalog_name=default_catalog,
        schema_name=default_schema,
        table_type=TableType.EXTERNAL,
        file_type=FileType.DELTA,
        columns=[
            Column(
                name="col1",
                data_type=DataType.INT,
                position=0,
                nullable=False,
            ),
            Column(
                name="col2",
                data_type=DataType.DOUBLE,
                position=1,
                nullable=True,
            ),
        ],
        storage_location="file://" + tmpfilepath,
    )

    for default_table in [default_external_table, default_managed_table]:
        table = client.get_table(
            catalog=default_catalog,
            schema=default_schema,
            table=default_table.name,
        )
        assert table.name == default_table.name
        assert table.catalog_name == default_table.catalog_name
        assert table.schema_name == default_table.schema_name
        assert table.table_type == default_table.table_type
        assert table.file_type == default_table.file_type

        assert table.storage_location is not None and table.storage_location != ""

        assert len(table.columns) == len(default_table.columns)
        for i in range(len(table.columns)):
            assert table.columns[i].name == default_table.columns[i].name
            assert table.columns[i].data_type == default_table.columns[i].data_type
            assert table.columns[i].position == default_table.columns[i].position
            assert table.columns[i].nullable == default_table.columns[i].nullable

    with pytest.raises(DoesNotExistError):
        client.get_table(
            catalog=default_catalog, schema=default_schema, table="safgsadgsadg"
        )

    with pytest.raises(DoesNotExistError):
        client.get_table(
            catalog=default_catalog,
            schema=default_schema + "asdasdas",
            table=default_external_table.name,
        )

    with pytest.raises(DoesNotExistError):
        client.get_table(
            catalog=default_catalog + "asdasdas",
            schema=default_schema,
            table=default_external_table.name,
        )

    tables = client.list_tables(catalog=default_catalog, schema=default_schema)
    assert len(tables) == 3
    for table in tables:
        assert table.catalog_name == default_catalog
        assert table.schema_name == default_schema

    with pytest.raises(DoesNotExistError):
        client.list_tables(catalog=default_catalog + "asdadsa", schema=default_schema)

    with pytest.raises(DoesNotExistError):
        client.list_tables(catalog=default_catalog, schema=default_schema + "asdasddas")

    created_table = client.create_table(new_external_table)
    assert created_table.name == new_external_table.name
    assert created_table.catalog_name == default_catalog
    assert created_table.schema_name == default_schema
    assert len(created_table.columns) == 2
    assert created_table.storage_location == new_external_table.storage_location
    assert created_table.created_at is not None
    assert created_table.updated_at is None
    assert created_table.table_id is not None

    assert len(client.list_tables(catalog=default_catalog, schema=default_schema)) == 4
