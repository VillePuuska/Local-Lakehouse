from pydantic import BaseModel, Field, ConfigDict, computed_field
from enum import Enum
import datetime
import uuid
import json


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


class Schema(BaseModel):
    """
    Holds all metadata for a schema in Unity Catalog.
    """

    name: str
    catalog_name: str
    comment: str | None = ""  # UC doesn't update a null comment properly so default ""
    properties: dict[str, str] = {}
    full_name: str | None = None
    created_at: datetime.datetime | None = None
    updated_at: datetime.datetime | None = None
    schema_id: uuid.UUID | None = None


class DataType(str, Enum):
    """
    Datatype of a column. Corresponding Unity Catalog model: ColumnTypeName.
    Possible values:
        - BOOLEAN
        - BYTE
        - SHORT
        - INT
        - LONG
        - FLOAT
        - DOUBLE
        - DATE
        - TIMESTAMP
        - TIMESTAMP_NTZ
        - STRING
        - BINARY
        - DECIMAL
        - INTERVAL
        - ARRAY
        - STRUCT
        - MAP
        - CHAR
        - NULL
        - USER_DEFINED_TYPE
        - TABLE_TYPE
    """

    BOOLEAN = "BOOLEAN"
    BYTE = "BYTE"
    SHORT = "SHORT"
    INT = "INT"
    LONG = "LONG"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    TIMESTAMP_NTZ = "TIMESTAMP_NTZ"
    STRING = "STRING"
    BINARY = "BINARY"
    DECIMAL = "DECIMAL"
    INTERVAL = "INTERVAL"
    ARRAY = "ARRAY"
    STRUCT = "STRUCT"
    MAP = "MAP"
    CHAR = "CHAR"
    NULL = "NULL"
    USER_DEFINED_TYPE = "USER_DEFINED_TYPE"
    TABLE_TYPE = "TABLE_TYPE"


class Column(BaseModel):
    """
    Model for a Column in Unity Catalog.
    """

    name: str
    data_type: DataType = Field(
        validation_alias="type_name", serialization_alias="type_name"
    )
    type_precision: int = 0  # TODO: handle this
    type_scale: int = 0  # TODO: handle this
    type_interval_type: int | None = None
    position: int
    comment: str | None = None
    nullable: bool
    partition_index: int | None = None

    @computed_field  # type: ignore[misc]
    @property
    def type_text(self) -> str:
        txt = self.data_type.value.lower()
        match txt:
            case "long":
                return "bigint"
            case "short":
                return "smallint"
            case "byte":
                return "tinyint"
            case _:
                return txt

    @computed_field  # type: ignore[misc]
    @property
    def type_json(self) -> str:
        json_type = self.data_type.value.lower()
        if json_type == "int":
            json_type = "integer"
        dct = {
            "name": self.name,
            "type": json_type,
            "nullable": self.nullable,
            "metadata": {},
        }
        return json.dumps(dct)

    model_config = ConfigDict(
        populate_by_name=True,
    )


class TableType(str, Enum):
    """
    Type of a table. Corresponding Unity Catalog model: TableType.
    Possible values:
        - MANAGED
        - EXTERNAL
    """

    MANAGED = "MANAGED"
    EXTERNAL = "EXTERNAL"


class FileType(str, Enum):
    """
    Type of a file. Corresponding Unity Catalog model: DataSourceFormat.
    Possible values:
        - DELTA
        - CSV
        - JSON
        - AVRO
        - PARQUET
        - ORC
        - TEXT
    """

    DELTA = "DELTA"
    CSV = "CSV"
    JSON = "JSON"
    AVRO = "AVRO"
    PARQUET = "PARQUET"
    ORC = "ORC"
    TEXT = "TEXT"


class Table(BaseModel):
    """
    Model for a Table in Unity Catalog.
    """

    name: str
    catalog_name: str
    schema_name: str
    table_type: TableType
    file_type: FileType = Field(validation_alias="data_source_format")
    columns: list[Column]
    storage_location: str | None = None
    comment: str | None = None
    properties: dict[str, str] = {}
    created_at: datetime.datetime | None = None
    updated_at: datetime.datetime | None = None
    table_id: uuid.UUID | None = None

    model_config = ConfigDict(
        populate_by_name=True,
    )
