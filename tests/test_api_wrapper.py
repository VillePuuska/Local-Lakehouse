import tempfile
import pytest
import polars as pl
from typing import Callable
from uchelper import (
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
from uchelper.uc_api_wrapper import overwrite_table


def test_catalogs_endpoint(client: UCClient):
    assert client.health_check()

    default_catalog = "unity"

    cat_name = "asdasdasdasfdsadgsa"
    cat_comment = "asd"
    cat_properties = {"prop1": "val1", "property 2": "foo bar"}
    catalog = Catalog(
        name=cat_name,
        comment=cat_comment,
        properties=cat_properties,
    )

    cat_name_update = "asdgnlsavnsadn"
    cat_comment_update = "ayo"
    cat_properties_update = {"prop new": "asddfg"}
    catalog_update = Catalog(name=cat_name_update, properties=cat_properties_update)
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
    assert cat.properties == cat_properties
    assert cat.id is not None
    assert cat.created_at is not None
    # NOTE: UC OSS PR #483 changed `updated_at` to be set when a catalog is first created
    # assert cat.updated_at is None

    assert len(client.list_catalogs()) == 2

    with pytest.raises(AlreadyExistsError):
        client.create_catalog(catalog)

    with pytest.raises(DoesNotExistError):
        client.update_catalog("xyz_this_cat_does_not_exist", catalog_update)

    # Try to update the default catalog with the info of the catalog we created;
    # this should not go through

    with pytest.raises(AlreadyExistsError):
        client.update_catalog(default_catalog, catalog)

    # Update the catalog name and properties; verify it was updated and the old catalog does not exist

    cat = client.update_catalog(cat_name, catalog_update)
    assert cat.name == cat_name_update
    assert cat.comment == cat_comment
    assert cat.properties == cat_properties_update

    assert len(client.list_catalogs()) == 2

    with pytest.raises(DoesNotExistError):
        client.get_catalog(cat_name)
    with pytest.raises(DoesNotExistError):
        client.delete_catalog(cat_name, False)
    assert len(client.list_catalogs()) == 2

    # Update just the comment but not the name or properties;
    # updating without changing the name used to be impossible due to a UC bug

    cat = client.update_catalog(cat_name_update, catalog_update2)
    assert cat.name == cat_name_update
    assert cat.comment == cat_comment_update
    assert cat.properties == cat_properties_update

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


def test_schemas_endpoint(client: UCClient):
    assert client.health_check()

    default_catalog = "unity"
    default_schema = "default"

    new_schema_name = "asdasdasdasfdsadgsa"
    new_schema_comment = "asd"
    new_schema_properties = {"prop1": "val1", "property 2": "foo bar"}
    new_schema = Schema(
        name=new_schema_name,
        catalog_name=default_catalog,
        comment=new_schema_comment,
        properties=new_schema_properties,
    )

    schema_name_update = "asdgnlsavnsadn"
    schema_properties_update = {"prop new": "asddfg"}
    schema_update = Schema(
        name=schema_name_update,
        catalog_name=default_catalog,
        properties=schema_properties_update,
    )

    schema_comment_update = "dddddddd"
    schema_update2 = Schema(
        name=schema_name_update,
        catalog_name=default_catalog,
        comment=schema_comment_update,
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
    assert schema.properties == new_schema_properties
    assert schema.created_at is not None
    # NOTE: UC OSS PR #483 changed `updated_at` to be set when a catalog is first created
    # assert schema.updated_at is None
    assert schema.schema_id is not None

    with pytest.raises(AlreadyExistsError):
        client.create_schema(schema=new_schema)

    schemas = client.list_schemas(catalog=default_catalog)
    assert len(schemas) == 2

    with pytest.raises(DoesNotExistError):
        client.update_schema(
            catalog=default_catalog,
            schema_name=schema_name_update,
            new_schema=schema_update,
        )

    # Update the schema name and properties; verify the schema was updated and the old schema does not exist

    schema = client.update_schema(
        catalog=default_catalog,
        schema_name=new_schema_name,
        new_schema=schema_update,
    )
    assert schema.full_name == default_catalog + "." + schema_name_update
    assert schema.comment == new_schema_comment  # comment was not changed
    assert schema.properties == schema_properties_update
    assert schema.updated_at is not None

    # Update only the comment; verify the schema was updated and the old schema does not exist

    schema = client.update_schema(
        catalog=default_catalog,
        schema_name=schema_name_update,
        new_schema=schema_update2,
    )
    assert schema.full_name == default_catalog + "." + schema_name_update  # unchanged
    assert schema.comment == schema_comment_update
    assert schema.properties == schema_properties_update  # unchanged
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


def test_tables_endpoint(client: UCClient):
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
        comment="External table",
        properties={"key1": "value1", "key2": "value2"},
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
        comment="Managed table",
        properties={"key1": "value1", "key2": "value2"},
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

    # Verify that two of the three default tables exist and get_table returns them

    for default_table in [default_external_table, default_managed_table]:
        assert_table_matches(client, default_table)

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

    # Verify that list_tables returns the four default tables

    tables = client.list_tables(catalog=default_catalog, schema=default_schema)
    assert len(tables) == 4
    for table in tables:
        assert table.catalog_name == default_catalog
        assert table.schema_name == default_schema

    with pytest.raises(DoesNotExistError):
        client.list_tables(catalog=default_catalog + "asdadsa", schema=default_schema)

    with pytest.raises(DoesNotExistError):
        client.list_tables(catalog=default_catalog, schema=default_schema + "asdasddas")

    # Create a new table and verify it gets added

    created_table = client.create_table(new_external_table)
    assert created_table.name == new_external_table.name
    assert created_table.catalog_name == default_catalog
    assert created_table.schema_name == default_schema
    assert len(created_table.columns) == 2
    assert created_table.storage_location == new_external_table.storage_location
    assert created_table.created_at is not None
    # NOTE: UC OSS PR #483 changed `updated_at` to be set when a table is first created
    # assert created_table.updated_at is None
    assert created_table.table_id is not None

    assert len(client.list_tables(catalog=default_catalog, schema=default_schema)) == 5

    # Test updating comment and properties of the last new table

    update_table_comment = created_table.model_copy(deep=True)
    update_table_comment.properties = None
    update_table_comment.comment = "avnpiavrn"
    client.update_table(
        catalog=default_catalog, schema=default_schema, table=update_table_comment
    )
    update_table_comment.properties = created_table.properties

    assert_table_matches(client, update_table_comment)

    update_table_props = created_table.model_copy(deep=True)
    update_table_props.properties = {"asdgasb": "opauvrn", "1234": "foobar"}
    update_table_props.comment = None
    client.update_table(
        catalog=default_catalog, schema=default_schema, table=update_table_props
    )
    update_table_props.comment = update_table_comment.comment

    assert_table_matches(client, update_table_props)

    # Test getting and setting default merge columns for a table

    assert update_table_props.default_merge_columns == []

    with pytest.raises(Exception):
        client.set_table_default_merge_columns(
            catalog=default_catalog,
            schema=default_schema,
            table=update_table_props.name,
            merge_columns=["col1", "column3"],
        )

    updated_table_merge_cols = client.set_table_default_merge_columns(
        catalog=default_catalog,
        schema=default_schema,
        table=update_table_props.name,
        merge_columns=["col1", "col2"],
    )
    assert updated_table_merge_cols.default_merge_columns == ["col1", "col2"]

    updated_table_merge_cols = client.set_table_default_merge_columns(
        catalog=default_catalog,
        schema=default_schema,
        table=update_table_props.name,
        merge_columns=["col1"],
    )
    assert updated_table_merge_cols.default_merge_columns == ["col1"]

    updated_table_merge_cols = client.set_table_default_merge_columns(
        catalog=default_catalog,
        schema=default_schema,
        table=update_table_props.name,
        merge_columns=[],
    )
    assert updated_table_merge_cols.default_merge_columns == []

    # Delete the table we created and verify it gets deleted

    client.delete_table(default_catalog, default_schema, new_external_table.name)

    assert len(client.list_tables(catalog=default_catalog, schema=default_schema)) == 4
    with pytest.raises(DoesNotExistError):
        client.get_table(default_catalog, default_schema, new_external_table.name)

    with pytest.raises(DoesNotExistError):
        client.delete_table(default_catalog, default_schema, new_external_table.name)


def test_overwrite_table(client: UCClient):
    assert client.health_check()

    uc_url = client.uc_url
    session = client.session

    default_catalog = "unity"
    default_schema = "default"

    default_table = client.get_table(
        catalog=default_catalog, schema=default_schema, table="numbers"
    )

    assert_table_matches(client, default_table)

    with pytest.raises(DoesNotExistError):
        nonexistent_table = default_table.model_copy(deep=True)
        nonexistent_table.name = "this_table_really_shouldnt_exist"
        overwrite_table(session=session, uc_url=uc_url, table=nonexistent_table)

    assert_table_matches(client, default_table)

    with pytest.raises(Exception):
        broken_table = default_table.model_copy(deep=True)
        broken_table.name = "this.name.will.fail"
        overwrite_table(session=session, uc_url=uc_url, table=broken_table)

    assert_table_matches(client, default_table)

    new_table_col_comment = "sagdfasdgasdg"
    new_table = default_table.model_copy(deep=True)
    new_table.columns[0].comment = new_table_col_comment
    overwrite_table(session=session, uc_url=uc_url, table=new_table)

    assert_table_matches(client, new_table)


def test_sync_delta_properties(
    client: UCClient,
    random_df: Callable[[], pl.DataFrame],
):
    assert client.health_check()

    default_catalog = "unity"
    default_schema = "default"
    table_name = "test_table"

    with tempfile.TemporaryDirectory() as tmpdir:
        df = random_df()

        client.create_as_table(
            df=df,
            catalog=default_catalog,
            schema=default_schema,
            name=table_name,
            file_type="delta",
            table_type="external",
            location="file://" + tmpdir,
        )

        assert (
            client.get_table(
                catalog=default_catalog,
                schema=default_schema,
                table=table_name,
            ).properties
            == {}
        )

        dt = client.get_delta_table(
            catalog=default_catalog,
            schema=default_schema,
            name=table_name,
        )
        dt.alter.add_constraint(constraints={"id_positive": "id > 0"})

        client.sync_delta_properties(
            catalog=default_catalog,
            schema=default_schema,
            name=table_name,
        )

        table = client.get_table(
            catalog=default_catalog,
            schema=default_schema,
            table=table_name,
        )
        assert table.properties is not None
        assert len(table.properties) == 1
        props_list = [(k, v) for k, v in table.properties.items()]
        assert props_list[0][0].startswith("delta.")
        assert props_list[0][0].endswith("id_positive")
        assert props_list[0][1] == "id > 0"

        table.properties["asd"] = "foo"
        client.update_table(catalog=default_catalog, schema=default_schema, table=table)
        client.sync_delta_properties(
            catalog=default_catalog,
            schema=default_schema,
            name=table_name,
        )

        table = client.get_table(
            catalog=default_catalog,
            schema=default_schema,
            table=table_name,
        )
        assert table.properties is not None
        assert len(table.properties) == 2
        props_list = [(k, v) for k, v in table.properties.items()]
        assert (
            props_list[0][0].startswith("delta.")
            and props_list[0][0].endswith("id_positive")
            and props_list[0][1] == "id > 0"
        ) or (
            props_list[1][0].startswith("delta.")
            and props_list[1][0].endswith("id_positive")
            and props_list[1][1] == "id > 0"
        )

        dt.alter.drop_constraint(name="id_positive")
        client.sync_delta_properties(
            catalog=default_catalog,
            schema=default_schema,
            name=table_name,
        )

        table = client.get_table(
            catalog=default_catalog,
            schema=default_schema,
            table=table_name,
        )
        assert table.properties == {"asd": "foo"}


def assert_table_matches(client: UCClient, default_table: Table):
    table = client.get_table(
        catalog=default_table.catalog_name,
        schema=default_table.schema_name,
        table=default_table.name,
    )
    assert table.name == default_table.name
    assert table.catalog_name == default_table.catalog_name
    assert table.schema_name == default_table.schema_name
    assert table.table_type == default_table.table_type
    assert table.file_type == default_table.file_type
    assert table.comment == default_table.comment
    assert table.properties == default_table.properties

    assert table.storage_location is not None and table.storage_location != ""

    assert len(table.columns) == len(default_table.columns)
    for i in range(len(table.columns)):
        assert table.columns[i].name == default_table.columns[i].name
        assert table.columns[i].data_type == default_table.columns[i].data_type
        assert table.columns[i].position == default_table.columns[i].position
        assert table.columns[i].nullable == default_table.columns[i].nullable
