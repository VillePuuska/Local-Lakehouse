"""
Functions for handling calling Unity Catalog REST API endpoints and parsing results.
"""

from .models import Catalog
import requests

api_path = "/api/2.1/unity-catalog"
catalog_endpoint = "/catalogs"


def health_check(session: requests.Session, uc_url: str) -> bool:
    """
    Checks that Unity Catalog is running at the specified address.
    """
    response = session.get(uc_url)

    if not response.ok:
        return False

    return "Hello, Unity Catalog!" in response.text


def create_catalog(session: requests.Session, uc_url: str, catalog: Catalog) -> Catalog:
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
    raise NotImplementedError


def delete_catalog(
    session: requests.Session, uc_url: str, name: str, force: bool
) -> bool:
    """
    Deletes the catalog with the specified name.`

    If `force == False`, then only deletes if the catalog is empty;
    if `force == True`, deletes the catalog even if it has schemas.

    Returns True/False indicating if a catalog was deleted.

    NOTE: force-flag for the REST API is bugged and does not prevent deleting a non-empty catalog.
    """
    url = uc_url + api_path + catalog_endpoint + "/" + name
    response = session.delete(url, params={"force": force})

    return response.ok


def list_catalogs(session: requests.Session, uc_url: str) -> list[Catalog]:
    """
    Returns a list of catalogs from the specified Unity Catalog.
    """
    catalogs = []
    token = None

    # NOTE: GET /catalogs pagination is bugged atm,
    # all catalogs are returned regardless of parameters
    # and next_page_token is always null.
    while True:
        response = session.get(
            uc_url + api_path + catalog_endpoint,
            params={"page_token": token},
        ).json()
        token = response["next_page_token"]
        catalogs.extend(
            [
                Catalog.model_validate(catalog, strict=False)
                for catalog in response["catalogs"]
            ]
        )
        # according to API spec, token should be null when there are no more pages,
        # but at least some endpoints are bugged and return "" instead of null
        if token is None or token == "":
            break

    return catalogs


def get_catalog(session: requests.Session, uc_url: str, name: str) -> Catalog | None:
    """
    Returns the info of the catalog with the specified name, if it exists.
    """
    url = uc_url + api_path + catalog_endpoint + "/" + name
    response = session.get(url)

    if not response.ok:
        return None

    return Catalog.model_validate(response.json())


def update_catalog(
    session: requests.Session, uc_url: str, name: str, catalog: Catalog
) -> Catalog | None:
    """
    Updates the catalog with the given name with the following fields from `catalog`:
        - name,
        - comment,
        - properties.
    Returns a Catalog with updated information, or None if the catalog did not exist.
    """
    raise NotImplementedError
