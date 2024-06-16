import requests
from .models import Catalog

api_path = "/api/2.1/unity-catalog"
catalog_endpoint = "/catalogs"


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
            response = self.session.get(self.uc_url)

            if not response.ok:
                return False

            return "Hello, Unity Catalog!" in response.text
        except requests.exceptions.ConnectionError:
            return False
        except Exception:
            raise

    def list_catalogs(self) -> list[Catalog]:
        """
        Returns a list of catalogs from the specified Unity Catalog.
        """
        catalogs = []
        token = None

        # NOTE: GET /catalogs pagination is bugged atm,
        # all catalogs are returned regardless of parameters
        # and next_page_token is always null.
        while True:
            response = self.session.get(
                self.uc_url + api_path + catalog_endpoint,
                params={"page_token": token},
            ).json()
            token = response["next_page_token"]
            catalogs.extend(
                [
                    Catalog.model_validate(catalog, strict=False)
                    for catalog in response["catalogs"]
                ]
            )
            if token is None:
                break

        return catalogs
