from pydantic import BaseModel
import datetime
import uuid


class Catalog(BaseModel):
    """
    Holds all metadata for a catalog in Unity Catalog.
    """

    name: str
    comment: str | None = None
    properties: dict[str, str] = {}
    created_at: datetime.datetime | None = None
    updated_at: datetime.datetime | None = None
    id: uuid.UUID | None = None
