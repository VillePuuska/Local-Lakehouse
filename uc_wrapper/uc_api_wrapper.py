"""
Functions for handling calling Unity Catalog REST API endpoints and parsing results.
"""

from .exceptions import AlreadyExistsException, DoesNotExistException
from .models import *
import requests
import json

api_path = "/api/2.1/unity-catalog"
catalog_endpoint = "/catalogs"
schemas_endpoint = "/schemas"


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
    Raises an AlreadyExistsException if a catalog with the name already exists.
    """
    data = {
        "name": catalog.name,
        "comment": catalog.comment,
        "properties": catalog.properties,
    }
    url = uc_url + api_path + catalog_endpoint
    response = session.post(
        url, data=json.dumps(data), headers={"Content-Type": "application/json"}
    )

    if not response.ok:
        response_dict = response.json()
        if response_dict.get("error_code", "").upper() == "ALREADY_EXISTS":
            raise AlreadyExistsException(response_dict.get("message", ""))
        raise Exception(
            f"Something went wrong. Server response:\n{response_dict.get('message', response.text)}"
        )

    return Catalog.model_validate_json(response.text)


def delete_catalog(
    session: requests.Session, uc_url: str, name: str, force: bool
) -> bool:
    """
    Deletes the catalog with the specified name.`

    If `force == False`, then only deletes if the catalog is empty;
    if `force == True`, deletes the catalog even if it has schemas.

    Returns True/False indicating if a catalog was deleted.

    NOTE: force-flag for the REST API is bugged and does not prevent deleting a non-empty catalog.

    TODO: Actually handle error responses from the server. After force-flag has been fixed.
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
    Returns a Catalog with updated information.
    Raises a DoesNotExistException if a catalog with the name already exists.
    """
    # BUG? Unity Catalog REST API does not allow updating a catalog without chaning the name:
    # - If the new_name is set and a catalog with the same name already exists, REST API returns ALREADY_EXISTS
    # - If the new_name is omitted, REST API returns INVALID_ARGUMENT
    if name == catalog.name:
        raise AlreadyExistsException(
            "Unity Catalog does not allow update a catalog without changing the name atm."
        )

    data = {
        "new_name": catalog.name,
        "comment": catalog.comment,
        "properties": catalog.properties,
    }
    url = uc_url + api_path + catalog_endpoint + "/" + name
    response = session.patch(
        url, data=json.dumps(data), headers={"Content-Type": "application/json"}
    )

    if not response.ok:
        response_dict = response.json()
        if response_dict.get("error_code", "").upper() == "NOT_FOUND":
            raise DoesNotExistException(response_dict.get("message", ""))
        raise Exception(
            f"Something went wrong. Server response:\n{response_dict.get('message', response.text)}"
        )

    return Catalog.model_validate_json(response.text)


def create_schema():
    raise NotImplementedError


def delete_schema():
    raise NotImplementedError


def get_schema():
    raise NotImplementedError


def list_schemas(session: requests.Session, uc_url: str, catalog: str) -> list[Schema]:
    """
    Returns a list of schemas in the specified catalog from Unity Catalog.
    """
    url = uc_url + api_path + schemas_endpoint

    schemas = []
    token = None

    # NOTE: listCatalogs pagination is not implemented in Unity Catalog yet
    # so this handling of pagination is not actually needed atm.
    while True:
        response = session.get(
            url,
            params={"page_token": token, "catalog_name": catalog},
        )
        response_dict = response.json()
        if not response.ok:
            if "NOT_FOUND" in response_dict["error_code"].upper():
                raise DoesNotExistException(response_dict.get("message", ""))
            raise Exception(
                f"Something went wrong. Server response:\n{response_dict.get('message', response.text)}"
            )

        token = response_dict["next_page_token"]
        schemas.extend(
            [
                Schema.model_validate(schema, strict=False)
                for schema in response_dict["schemas"]
            ]
        )
        if token is None or token == "":
            break

    return schemas


def update_schema():
    raise NotImplementedError
