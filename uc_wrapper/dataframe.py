import polars as pl
from enum import Enum
from .models import Table


class WriteMode(str, Enum):
    APPEND = "APPEND"
    OVERWRITE = "OVERWRITE"


class SchemaEvolution(str, Enum):
    STRICT = "STRICT"
    UNION = "UNION"
    OVERWRITE = "OVERWRITE"


def read_table(table: Table) -> pl.DataFrame:
    raise NotImplementedError


def scan_table(table: Table) -> pl.LazyFrame:
    raise NotImplementedError


def write_table(
    table: Table,
    df: pl.DataFrame | pl.LazyFrame,
    mode: WriteMode = WriteMode.APPEND,
    schema_evolution: SchemaEvolution = SchemaEvolution.STRICT,
) -> None:
    raise NotImplementedError
