import requests
from .models import Catalog, Schema
from .uc_api_wrapper import (
    create_catalog,
    create_schema,
    delete_catalog,
    delete_schema,
    get_catalog,
    get_schema,
    health_check,
    list_catalogs,
    list_schemas,
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
        return health_check(self.session, self.uc_url)

    def create_catalog(self, catalog: Catalog) -> Catalog:
        """
        Creates a new catalog with the following fields specified in the parameter `catalog`:
            - name,
            - comment,
            - properties.
        Returns a new Catalog with the following fields added:
            - created_at,
            - id.
        Raises an Exception if a catalog with the name already exists.
        """
        return create_catalog(self.session, self.uc_url, catalog)

    def delete_catalog(self, name: str, force: bool) -> bool:
        """
        Deletes the catalog with the specified name.`

        If `force == False`, then only deletes if the catalog is empty;
        if `force == True`, deletes the catalog even if it has schemas.

        Returns True/False indicating if a catalog was deleted.
        """
        return delete_catalog(self.session, self.uc_url, name, force)

    def list_catalogs(self) -> list[Catalog]:
        """
        Returns a list of catalogs from the specified Unity Catalog.
        """
        return list_catalogs(self.session, self.uc_url)

    def get_catalog(self, name: str) -> Catalog:
        """
        Returns the info of the catalog with the specified name, if it exists.
        Raises a DoesNotExistException if a catalog with the name does not exist.
        """
        return get_catalog(self.session, self.uc_url, name)

    def update_catalog(self, name: str, catalog: Catalog) -> Catalog:
        """
        Updates the catalog with the given name with the following fields from `catalog`:
            - name,
            - comment,
            - properties.
        Returns a Catalog with updated information.
        Raises a DoesNotExistException if a catalog with the name does not exist.
        """
        return update_catalog(self.session, self.uc_url, name, catalog)

    def create_schema(self, schema: Schema) -> Schema:
        """
        Creates a new schema with the following fields specified in the parameter `schema`:
            - name,
            - catalog_name,
            - comment,
            - properties.
        Returns a new Schema with the remaining fields populated.
        Raises an AlreadyExistsException if a schema with the name already exists in the same catalog.
        """
        return create_schema(self.session, self.uc_url, schema)

    def delete_schema(self, catalog: str, schema: str):
        """
        Deletes the schema in the catalog.
        Raises a DoesNotExistError if the schema did not exist.
        """
        delete_schema(self.session, self.uc_url, catalog, schema)

    def get_schema(self, catalog: str, schema: str) -> Schema:
        """
        Returns the info of the schema in the catalog, if it exists.
        Raises a DoesNotExistException if the schema or catalog does not exist.
        """
        return get_schema(self.session, self.uc_url, catalog, schema)

    def list_schemas(self, catalog: str) -> list[Schema]:
        """
        Returns a list of schemas in the specified catalog from Unity Catalog.
        """
        return list_schemas(self.session, self.uc_url, catalog)

    def update_schema(self):
        raise NotImplementedError
