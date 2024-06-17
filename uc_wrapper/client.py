import requests
from .models import Catalog
from .uc_api_wrapper import (
    health_check,
    create_catalog,
    delete_catalog,
    list_catalogs,
    get_catalog,
    update_catalog,
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
        try:
            return health_check(self.session, self.uc_url)
        except requests.exceptions.ConnectionError:
            return False
        except Exception:
            raise

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

    def get_catalog(self, name: str) -> Catalog | None:
        """
        Returns the info of the catalog with the specified name, if it exists.
        """
        return get_catalog(self.session, self.uc_url, name)

    def update_catalog(self, name: str, catalog: Catalog) -> Catalog | None:
        """
        Updates the catalog with the given name with the following fields from `catalog`:
            - name,
            - comment,
            - properties.
        Returns a Catalog with updated information, or None if the catalog did not exist.
        """
        return update_catalog(self.session, self.uc_url, name, catalog)
