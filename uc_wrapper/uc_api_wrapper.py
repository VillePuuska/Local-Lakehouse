"""
Functions for handling calling Unity Catalog REST API endpoints and parsing results.
"""

from .exceptions import AlreadyExistsError, DoesNotExistError
from .models import *
import requests
import json

# error_code the Unity Catalog REST API returns if something was not found
SERVER_NOT_FOUND_ERROR = "NOT_FOUND"
# error_code the Unity Catalog REST API returns if something to be created already exists
SERVER_ALREADY_EXISTS_ERROR = "ALREADY_EXISTS"

JSON_HEADER = {"Content-Type": "application/json"}

api_path = "/api/2.1/unity-catalog"
catalogs_endpoint = "/catalogs"
schemas_endpoint = "/schemas"
tables_endpoint = "/tables"


def _check_already_exists_response(response: requests.Response):
    """
    Helper function to check if Unity Catalog responded with an ALREADY_EXISTS error.
    Raises an AlreadyExistsError if yes.
    """
    if not response.ok:
        response_dict = response.json()
        if response_dict.get("error_code", "").upper() == SERVER_ALREADY_EXISTS_ERROR:
            raise AlreadyExistsError(response_dict.get("message", ""))


def _check_does_not_exist_response(response: requests.Response):
    """
    Helper function to check if Unity Catalog responded with a NOT_FOUND error.
    Raises a DoesNotExistError if yes.
    """
    if not response.ok:
        response_dict = response.json()
        if response_dict.get("error_code", "").upper() == SERVER_NOT_FOUND_ERROR:
            raise DoesNotExistError(response_dict.get("message", ""))


def _check_response_failed(response: requests.Response):
    """
    Helper function to raise an Exception with the error message if Unity Catalog responded
    with any error.
    Raises an Exception.
    """
    if not response.ok:
        response_dict = response.json()
        raise Exception(
            f"Something went wrong. Server response:\n{response_dict.get('message', response.text)}"
        )


def health_check(session: requests.Session, uc_url: str) -> bool:
    """
    Checks that Unity Catalog is running at the specified address.
    """
    try:
        response = session.get(uc_url)

        if not response.ok:
            return False

        return "Hello, Unity Catalog!" in response.text

    except requests.exceptions.ConnectionError:
        return False
    except Exception:
        raise


def create_catalog(session: requests.Session, uc_url: str, catalog: Catalog) -> Catalog:
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
    data = {
        "name": catalog.name,
        "comment": catalog.comment,
        "properties": catalog.properties,
    }
    url = uc_url + api_path + catalogs_endpoint
    response = session.post(url, data=json.dumps(data), headers=JSON_HEADER)

    _check_already_exists_response(response=response)
    _check_response_failed(response=response)

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
    url = uc_url + api_path + catalogs_endpoint + "/" + name
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
            uc_url + api_path + catalogs_endpoint,
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


def get_catalog(session: requests.Session, uc_url: str, name: str) -> Catalog:
    """
    Returns the info of the catalog with the specified name, if it exists.
    Raises a DoesNotExistError if a catalog with the name does not exist.
    """
    url = uc_url + api_path + catalogs_endpoint + "/" + name
    response = session.get(url)

    _check_does_not_exist_response(response=response)
    _check_response_failed(response=response)

    return Catalog.model_validate_json(response.text)


def update_catalog(
    session: requests.Session, uc_url: str, name: str, catalog: Catalog
) -> Catalog:
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
    # BUG? Unity Catalog REST API does not allow updating a catalog without changing the name:
    # - If the new_name is set and a catalog with the same name already exists, REST API returns ALREADY_EXISTS
    # - If the new_name is omitted, REST API returns INVALID_ARGUMENT
    if name == catalog.name:
        raise AlreadyExistsError(
            "Unity Catalog does not allow update a catalog without changing the name atm."
        )

    data = {
        "new_name": catalog.name,
        "comment": catalog.comment,
        "properties": catalog.properties,
    }
    url = uc_url + api_path + catalogs_endpoint + "/" + name
    response = session.patch(url, data=json.dumps(data), headers=JSON_HEADER)

    _check_does_not_exist_response(response=response)
    _check_response_failed(response=response)

    return Catalog.model_validate_json(response.text)


def create_schema(session: requests.Session, uc_url: str, schema: Schema) -> Schema:
    """
    Creates a new schema with the following fields specified in the parameter `schema`:
        - name,
        - catalog_name,
        - comment,
        - properties.
    Returns a new Schema with the remaining fields populated.
    Raises an AlreadyExistsError if a schema with the name already exists in the same catalog.
    """
    url = uc_url + api_path + schemas_endpoint
    data = {
        "name": schema.name,
        "catalog_name": schema.catalog_name,
        "comment": schema.comment,
        "properties": schema.properties,
    }
    response = session.post(url, data=json.dumps(data), headers=JSON_HEADER)

    _check_already_exists_response(response=response)
    _check_response_failed(response=response)

    return Schema.model_validate_json(response.text)


def delete_schema(session: requests.Session, uc_url: str, catalog: str, schema: str):
    """
    Deletes the schema in the catalog.
    Raises a DoesNotExistError if the schema did not exist.
    """
    url = uc_url + api_path + schemas_endpoint + "/" + catalog + "." + schema
    response = session.delete(url)

    _check_does_not_exist_response(response=response)
    _check_response_failed(response=response)


def get_schema(
    session: requests.Session, uc_url: str, catalog: str, schema: str
) -> Schema:
    """
    Returns the info of the schema in the catalog, if it exists.
    Raises a DoesNotExistException if the schema or catalog does not exist.
    """
    url = uc_url + api_path + schemas_endpoint + "/" + catalog + "." + schema
    response = session.get(url)

    _check_does_not_exist_response(response=response)
    _check_response_failed(response=response)

    return Schema.model_validate_json(response.text)


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

        _check_does_not_exist_response(response=response)
        _check_response_failed(response=response)

        response_dict = response.json()
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


def update_schema(
    session: requests.Session,
    uc_url: str,
    catalog: str,
    schema_name: str,
    new_schema: Schema,
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
    url = uc_url + api_path + schemas_endpoint + "/" + catalog + "." + schema_name
    data = {
        "comment": new_schema.comment,
        "properties": new_schema.properties,
        "new_name": new_schema.name,
    }
    response = session.patch(url=url, data=json.dumps(data), headers=JSON_HEADER)

    _check_does_not_exist_response(response=response)
    _check_already_exists_response(response=response)
    _check_response_failed(response=response)

    return Schema.model_validate_json(response.text)


def create_table(session: requests.Session, uc_url: str, table: Table) -> Table:
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
    raise NotImplementedError


def delete_table(
    session: requests.Session, uc_url: str, catalog: str, schema: str, table: str
):
    """
    Deletes the table.
    Raises a DoesNotExistError if the table did not exist.
    """
    raise NotImplementedError


def get_table(
    session: requests.Session, uc_url: str, catalog: str, schema: str, table: str
) -> Table:
    """
    Returns the info of the table, if it exists.
    Raises a DoesNotExistException if the table does not exist.
    """
    url = (
        uc_url + api_path + tables_endpoint + "/" + catalog + "." + schema + "." + table
    )
    response = session.get(url)

    _check_does_not_exist_response(response=response)
    _check_response_failed(response=response)

    return Table.model_validate_json(response.text)


def list_tables(
    session: requests.Session, uc_url: str, catalog: str, schema: str
) -> list[Table]:
    """
    Returns a list of tables in the specified catalog.schema from Unity Catalog.
    """
    raise NotImplementedError
