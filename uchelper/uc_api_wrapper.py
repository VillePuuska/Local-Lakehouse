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
    Deletes the catalog with the specified name.

    If `force == False`, then only deletes if the catalog is empty;
    if `force == True`, deletes the catalog even if it has schemas.

    Returns True/False indicating if a catalog was deleted.
    Raises a DoesNotExistError if a catalog with the name does not exist.
    """
    url = uc_url + api_path + catalogs_endpoint + "/" + name
    # If we don't convert the boolean `force` to a _lowercase_ string,
    # requests sets the query parameter to ?force=True or ?force=False
    # which the Unity Catalog server does not parse properly.
    response = session.delete(url, params={"force": ("true" if force else "false")})

    _check_does_not_exist_response(response=response)

    if response.ok:
        return True

    if "Cannot delete catalog with schemas" in response.text:
        return False

    _check_response_failed(response=response)

    return False  # superfluous return that is never reached just to make mypy happy


def list_catalogs(session: requests.Session, uc_url: str) -> list[Catalog]:
    """
    Returns a list of catalogs from the specified Unity Catalog.
    """
    catalogs = []
    token = None

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
        # but at least some endpoints have been bugged and returned "" instead of null
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
    Raises an AlreadyExistsError if there is a catalog with the new name.
    """
    data = {
        "new_name": (catalog.name if catalog.name != name else None),
        "comment": catalog.comment,
        "properties": catalog.properties,
    }
    url = uc_url + api_path + catalogs_endpoint + "/" + name
    response = session.patch(url, data=json.dumps(data), headers=JSON_HEADER)

    _check_already_exists_response(response=response)
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


def delete_schema(
    session: requests.Session, uc_url: str, catalog: str, schema: str, force: bool
) -> bool:
    """
    Deletes the schema in the catalog.

    If `force == False`, then only deletes if the schema is empty;
    if `force == True`, deletes the schema even if it has tables.

    Returns True/False indicating if a schema was deleted.
    Raises a DoesNotExistError if a schema with the name does not exist.
    """
    url = uc_url + api_path + schemas_endpoint + "/" + catalog + "." + schema
    # If we don't convert the boolean `force` to a _lowercase_ string,
    # requests sets the query parameter to ?force=True or ?force=False
    # which the Unity Catalog server does not parse properly.
    response = session.delete(url, params={"force": ("true" if force else "false")})

    _check_does_not_exist_response(response=response)

    if response.ok:
        return True

    if "Cannot delete schema with tables" in response.text:
        return False

    _check_response_failed(response=response)

    return False  # superfluous return that is never reached just to make mypy happy


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
    Raises an AlreadyExistsError if there already exists a schema with the new name
    in the same catalog.
    """
    url = uc_url + api_path + schemas_endpoint + "/" + catalog + "." + schema_name
    data = {
        "comment": new_schema.comment,
        "properties": new_schema.properties,
        "new_name": (new_schema.name if new_schema.name != schema_name else None),
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
    url = uc_url + api_path + tables_endpoint
    data = {
        "name": table.name,
        "catalog_name": table.catalog_name,
        "schema_name": table.schema_name,
        "table_type": table.table_type,
        "data_source_format": table.file_type,
        "columns": [col.model_dump(by_alias=True) for col in table.columns],
        "storage_location": table.storage_location,
        "comment": table.comment,
        "properties": table.properties,
    }
    response = session.post(url, data=json.dumps(data), headers=JSON_HEADER)

    _check_already_exists_response(response=response)
    _check_response_failed(response=response)

    return Table.model_validate_json(response.text)


def delete_table(
    session: requests.Session, uc_url: str, catalog: str, schema: str, table: str
) -> None:
    """
    Deletes the table.
    Raises a DoesNotExistError if the table did not exist.
    """
    url = (
        uc_url + api_path + tables_endpoint + "/" + catalog + "." + schema + "." + table
    )
    response = session.delete(url)

    _check_does_not_exist_response(response=response)
    _check_response_failed(response=response)


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
    url = uc_url + api_path + tables_endpoint

    tables = []
    token = None

    while True:
        response = session.get(
            url,
            params={
                "page_token": token,
                "catalog_name": catalog,
                "schema_name": schema,
            },
        )

        _check_does_not_exist_response(response=response)
        _check_response_failed(response=response)

        response_dict = response.json()
        token = response_dict["next_page_token"]
        tables.extend(
            [
                Table.model_validate(table, strict=False)
                for table in response_dict["tables"]
            ]
        )
        if token is None or token == "":
            break

    return tables
