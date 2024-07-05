from pydantic import BaseModel, Field
from enum import Enum
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


class DataType(Enum):
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
    name: str
    type_text: str | None = None
    type_json: str | None = None
    data_type: DataType = Field(alias="type_name")
    type_precision: int | None = None
    type_scale: int | None = None
    type_interval_type: int | None = None
    position: int
    comment: str | None = None
    nullable: bool
    partition_index: int | None = None


class TableType(Enum):
    """
    Type of a table. Corresponding Unity Catalog model: TableType.
    Possible values:
        - MANAGED
        - EXTERNAL
    """

    MANAGED = "MANAGED"
    EXTERNAL = "EXTERNAL"


class FileType(Enum):
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
    file_type: FileType = Field(alias="data_source_format")
    columns: list[Column]
    storage_location: str | None = None
    comment: str | None = None
    properties: dict[str, str] = {}
    created_at: datetime.datetime | None = None
    updated_at: datetime.datetime | None = None
    table_id: uuid.UUID | None = None
