from pydantic import BaseModel
import datetime


class Catalog(BaseModel):
    """
    Holds all metadata for a catalog in Unity Catalog.
    """

    name: str
    comment: str | None
    properties: dict[str, str]
    created_at: datetime.datetime | None
    updated_at: datetime.datetime | None
    id: str | None
