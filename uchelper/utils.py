from .models import TableType, FileType
from .dataframe import WriteMode, SchemaEvolution
from .exceptions import UnsupportedOperationError
from typing import Literal


def literal_to_tabletype(lit: Literal["managed", "external"]) -> TableType:
    match lit:
        case "managed":
            return TableType.MANAGED
        case "external":
            return TableType.EXTERNAL
        case _:
            raise UnsupportedOperationError(f"{lit} is not a valid TableType.")


def literal_to_filetype(
    lit: Literal["delta", "csv", "json", "avro", "parquet", "orc", "text"]
) -> FileType:
    match lit:
        case "delta":
            return FileType.DELTA
        case "csv":
            return FileType.CSV
        case "json":
            return FileType.JSON
        case "avro":
            return FileType.AVRO
        case "parquet":
            return FileType.PARQUET
        case "orc":
            return FileType.ORC
        case "text":
            return FileType.TEXT
        case _:
            raise UnsupportedOperationError(f"{lit} is not a valid FileType.")


def literal_to_writemode(lit: Literal["append", "overwrite"]) -> WriteMode:
    match lit:
        case "append":
            return WriteMode.APPEND
        case "overwrite":
            return WriteMode.OVERWRITE
        case _:
            raise UnsupportedOperationError(f"{lit} is not a valid WriteMode.")


def literal_to_schemaevolution(
    lit: Literal["strict", "merge", "overwrite"]
) -> SchemaEvolution:
    match lit:
        case "strict":
            return SchemaEvolution.STRICT
        case "merge":
            return SchemaEvolution.MERGE
        case "overwrite":
            return SchemaEvolution.OVERWRITE
        case _:
            raise UnsupportedOperationError(f"{lit} is not a valid SchemaEvolution.")
