import polars as pl
from enum import Enum
from .exceptions import UnsupportedOperationError, SchemaMismatchError
from .models import Table, FileType, Column, DataType


class WriteMode(str, Enum):
    APPEND = "APPEND"
    OVERWRITE = "OVERWRITE"


class SchemaEvolution(str, Enum):
    STRICT = "STRICT"
    UNION = "UNION"
    OVERWRITE = "OVERWRITE"


def polars_type_to_uc_type(t: pl.DataType) -> DataType:
    match t:
        case pl.Decimal:
            return DataType.DECIMAL
        case pl.Float32:
            return DataType.FLOAT
        case pl.Float64:
            return DataType.DOUBLE
        case pl.Int8:
            return DataType.BYTE
        case pl.Int16:
            return DataType.SHORT
        case pl.Int32:
            return DataType.INT
        case pl.Int64:
            return DataType.LONG
        case pl.Date:
            return DataType.DATE
        case pl.Datetime:
            return DataType.TIMESTAMP
        case pl.Array:
            return DataType.ARRAY
        case pl.List:
            return DataType.ARRAY
        case pl.Struct:
            return DataType.STRUCT
        case pl.String | pl.Utf8:
            return DataType.STRING
        case pl.Binary:
            return DataType.BINARY
        case pl.Boolean:
            return DataType.BOOLEAN
        case pl.Null:
            return DataType.NULL
        case _:
            raise UnsupportedOperationError(f"Unsupported datatype: {type(t)}")


def df_schema_to_uc_schema(df: pl.DataFrame | pl.LazyFrame) -> list[Column]:
    res = []
    for i, (col_name, col_type) in enumerate(df.schema.items()):
        res.append(
            Column(
                name=col_name,
                data_type=polars_type_to_uc_type(col_type),
                position=i,
                nullable=True,
            )
        )
    return res


def check_schema_equality(left: list[Column], right: list[Column]) -> bool:
    left = sorted(left, key=lambda x: x.position)
    right = sorted(right, key=lambda x: x.position)
    for left_col, right_col in zip(left, right):
        if left_col.name != right_col.name:
            return False
        if left_col.data_type != right_col.data_type:
            return False
    return True


def read_table(table: Table) -> pl.DataFrame:
    path = table.storage_location
    assert path is not None
    if not path.startswith("file://"):
        raise UnsupportedOperationError("Only local storage is supported.")
    path = path.removeprefix("file://")
    match table.file_type:
        case FileType.DELTA:
            df = pl.read_delta(source=path)

        case FileType.PARQUET:
            df = pl.read_parquet(source=path)

        case FileType.CSV:
            df = pl.read_csv(source=path)

        case FileType.AVRO:
            df = pl.read_avro(source=path)

        case _:
            raise NotImplementedError

    return df


def scan_table(table: Table) -> pl.LazyFrame:
    path = table.storage_location
    assert path is not None
    if not path.startswith("file://"):
        raise UnsupportedOperationError("Only local storage is supported.")
    path = path.removeprefix("file://")
    match table.file_type:
        case FileType.DELTA:
            df = pl.scan_delta(source=path)

        case FileType.PARQUET:
            df = pl.scan_parquet(source=path)

        case FileType.CSV:
            df = pl.scan_csv(source=path)

        case FileType.AVRO:
            raise UnsupportedOperationError("scan is not supported for Avro.")

        case _:
            raise NotImplementedError

    return df


def write_table(
    table: Table,
    df: pl.DataFrame,
    mode: WriteMode | None = WriteMode.APPEND,
    schema_evolution: SchemaEvolution | None = SchemaEvolution.STRICT,
) -> list[Column] | None:
    """
    Writes the Polars DataFrame `df` to the location of `table`.

    If `mode` is APPEND, depending on the `schema_evolution` parameter, if the schema
    stored in Unity Catalog needs to be updated, returns the new list of Columns.
    If the schema does not need to be updated, returns None.

    If `mode` is OVERWRITE, the function returns the list of Columns if it doesn't
    match the previous schema in Unity Catalog. Otherwise returns None.

    `schema_evolution` is completely ignored if `mode` is OVERWRITE.

    In short: if this function returns None, Unity Catalog does not need an update;
    otherwise update the schema in Unity Catalog with the returned list of Columns.
    """
    path = table.storage_location
    assert path is not None
    if not path.startswith("file://"):
        raise UnsupportedOperationError("Only local storage is supported.")
    path = path.removeprefix("file://")
    match mode, table.file_type, schema_evolution:
        case WriteMode.APPEND, FileType.DELTA, SchemaEvolution.STRICT:
            df_uc_schema = df_schema_to_uc_schema(df=df)
            if not check_schema_equality(left=df_uc_schema, right=table.columns):
                raise SchemaMismatchError(
                    f"Schema evolution is set to strict but schemas do not match: {df_uc_schema} VS {table.columns}"
                )
            df.write_delta(target=path, mode="append")
            return None

        case WriteMode.APPEND, FileType.DELTA, SchemaEvolution.UNION:
            raise NotImplementedError

        case WriteMode.APPEND, FileType.DELTA, SchemaEvolution.OVERWRITE:
            raise NotImplementedError

        case WriteMode.APPEND, _:
            raise UnsupportedOperationError(
                "Appending is only supported with Delta Lake tables."
            )

        case _, FileType.DELTA, _:
            raise NotImplementedError

        case _, FileType.PARQUET, _:
            raise NotImplementedError

        case _, FileType.CSV, _:
            raise NotImplementedError

        case _, FileType.AVRO, _:
            raise NotImplementedError

        case _:
            raise NotImplementedError
