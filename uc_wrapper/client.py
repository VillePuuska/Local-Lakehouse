import requests
from .models import Catalog, Schema, Table
from .uc_api_wrapper import (
    create_catalog,
    create_schema,
    create_table,
    delete_catalog,
    delete_schema,
    delete_table,
    get_catalog,
    get_schema,
    get_table,
    health_check,
    list_catalogs,
    list_schemas,
    list_tables,
    update_catalog,
    update_schema,
)


class UCClient:
    """
    A UCCLient object
        - holds the connections to the Unity Catalog (requests Session, DuckDB connection),
        - exposes methods for interacting with the Unity Catalog.
    """

    def __init__(self, uc_url: str = "http://localhost:8080") -> None:
        self.uc_url = uc_url.removesuffix("/")
        self.session = requests.Session()

    def health_check(self) -> bool:
        """
        Checks that Unity Catalog is running at the specified address.
        """
        return health_check(session=self.session, uc_url=self.uc_url)

    def create_catalog(self, catalog: Catalog) -> Catalog:
        """
        Creates a new catalog with the following fields specified in the parameter `catalog`:
            - name,
            - comment,
            - properties.
        Returns a new Catalog with the following fields added:
            - created_at,
            - id.
        Raises an AlreadyExistsError if a catalog with the name already exists.
        """
        return create_catalog(session=self.session, uc_url=self.uc_url, catalog=catalog)

    def delete_catalog(self, name: str, force: bool) -> bool:
        """
        Deletes the catalog with the specified name.`

        If `force == False`, then only deletes if the catalog is empty;
        if `force == True`, deletes the catalog even if it has schemas.

        Returns True/False indicating if a catalog was deleted.
        """
        return delete_catalog(
            session=self.session, uc_url=self.uc_url, name=name, force=force
        )

    def list_catalogs(self) -> list[Catalog]:
        """
        Returns a list of catalogs from the specified Unity Catalog.
        """
        return list_catalogs(session=self.session, uc_url=self.uc_url)

    def get_catalog(self, name: str) -> Catalog:
        """
        Returns the info of the catalog with the specified name, if it exists.
        Raises a DoesNotExistError if a catalog with the name does not exist.
        """
        return get_catalog(session=self.session, uc_url=self.uc_url, name=name)

    def update_catalog(self, name: str, catalog: Catalog) -> Catalog:
        """
        Updates the catalog with the given name with the following fields from `catalog`:
            - name,
            - comment,
            - properties.
        Returns a Catalog with updated information.
        Raises a DoesNotExistError if a catalog with the name does not exist.
        Raises an AlreadyExistsError if the new name is the same as the old name. Unity Catalog
        does not allow updating and keeping the same name atm.
        """
        return update_catalog(
            session=self.session, uc_url=self.uc_url, name=name, catalog=catalog
        )

    def create_schema(self, schema: Schema) -> Schema:
        """
        Creates a new schema with the following fields specified in the parameter `schema`:
            - name,
            - catalog_name,
            - comment,
            - properties.
        Returns a new Schema with the remaining fields populated.
        Raises an AlreadyExistsError if a schema with the name already exists in the same catalog.
        """
        return create_schema(session=self.session, uc_url=self.uc_url, schema=schema)

    def delete_schema(self, catalog: str, schema: str):
        """
        Deletes the schema in the catalog.
        Raises a DoesNotExistError if the schema did not exist.
        """
        delete_schema(
            session=self.session, uc_url=self.uc_url, catalog=catalog, schema=schema
        )

    def get_schema(self, catalog: str, schema: str) -> Schema:
        """
        Returns the info of the schema in the catalog, if it exists.
        Raises a DoesNotExistError if the schema or catalog does not exist.
        """
        return get_schema(
            session=self.session, uc_url=self.uc_url, catalog=catalog, schema=schema
        )

    def list_schemas(self, catalog: str) -> list[Schema]:
        """
        Returns a list of schemas in the specified catalog from Unity Catalog.
        """
        return list_schemas(session=self.session, uc_url=self.uc_url, catalog=catalog)

    def update_schema(
        self, catalog: str, schema_name: str, new_schema: Schema
    ) -> Schema:
        """
        Updates the schema with the given name in the given catalog with the following
        fields from `new_schema`:
            - name,
            - comment,
            - properties.
        Returns a Schema with updated information.
        Raises a DoesNotExistError if the schema does not exist.
        Raises an AlreadyExistsError if the new name is the same as the old name. Unity Catalog
        does not allow updating and keeping the same name atm.
        """
        return update_schema(
            uc_url=self.uc_url,
            session=self.session,
            catalog=catalog,
            schema_name=schema_name,
            new_schema=new_schema,
        )

    def create_table(self, table: Table) -> Table:
        """
        Creates a new table with the following fields specified in the parameter `table`:
            - name,
            - catalog_name,
            - schema_name,
            - table_type,
            - file_type,
            - columns,
            - storage_location (for EXTERNAL tables),
            - comment,
            - properties.
        Returns a new Table with the remaining fields populated.
        Raises an AlreadyExistsError if a Table with the name already exists in the same catalog.
        """
        return create_table(session=self.session, uc_url=self.uc_url, table=table)

    def delete_table(
        self,
        catalog: str,
        schema: str,
        table: str,
    ):
        """
        Deletes the table.
        Raises a DoesNotExistError if the table did not exist.
        """
        return delete_table(
            session=self.session,
            uc_url=self.uc_url,
            catalog=catalog,
            schema=schema,
            table=table,
        )

    def get_table(
        self,
        catalog: str,
        schema: str,
        table: str,
    ) -> Table:
        """
        Returns the info of the table, if it exists.
        Raises a DoesNotExistException if the table does not exist.
        """
        return get_table(
            session=self.session,
            uc_url=self.uc_url,
            catalog=catalog,
            schema=schema,
            table=table,
        )

    def list_tables(self, catalog: str, schema: str) -> list[Table]:
        """
        Returns a list of tables in the specified catalog.schema from Unity Catalog.
        """
        return list_tables(
            session=self.session, uc_url=self.uc_url, catalog=catalog, schema=schema
        )
