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
