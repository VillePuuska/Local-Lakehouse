import polars as pl
import os
import time
import uuid
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


def polars_type_to_uc_type(t: pl.PolarsDataType) -> DataType:
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
            raise UnsupportedOperationError(f"Unsupported datatype: {t}")


def uc_type_to_polars_type(t: DataType) -> pl.PolarsDataType:
    match t:
        case DataType.BOOLEAN:
            return pl.Boolean
        case DataType.BYTE:
            return pl.Int8
        case DataType.SHORT:
            return pl.Int16
        case DataType.INT:
            return pl.Int32
        case DataType.LONG:
            return pl.Int64
        case DataType.FLOAT:
            return pl.Float32
        case DataType.DOUBLE:
            return pl.Float64
        case DataType.DATE:
            return pl.Date
        case DataType.TIMESTAMP:
            return pl.Datetime
        case DataType.STRING:
            return pl.String
        case DataType.BINARY:
            return pl.Binary
        case DataType.DECIMAL:
            return pl.Decimal
        case DataType.ARRAY:
            return pl.Array
        case DataType.STRUCT:
            return pl.Struct
        case DataType.CHAR:
            return pl.String
        case DataType.NULL:
            return pl.Null
        case _:
            raise UnsupportedOperationError(f"Unsupported datatype: {t.value}")


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


def get_partition_columns(cols: list[Column]) -> list[Column]:
    partition_cols = [col for col in cols if col.partition_index is not None]
    # mypy doesn't understand that the type of x.partition_index is int in the following lambda
    # and instead thinks it's int | None and complains if we don't just ignore it.
    # TODO: find a better workaroudn than just ignoring?
    return sorted(partition_cols, key=lambda x: x.partition_index)  # type: ignore


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
            partition_cols = get_partition_columns(table.columns)
            if len(partition_cols) == 0:
                df = pl.read_parquet(source=path)
            else:
                # TODO: There HAS to be a nicer way to do this. Try with Polars >1.0?
                df = pl.read_parquet(
                    source=os.path.join(
                        path, *["**" for _ in range(len(partition_cols))], "*.parquet"
                    ),
                    hive_schema={
                        col.name: uc_type_to_polars_type(col.data_type)
                        for col in partition_cols
                    },
                )

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
            partition_cols = get_partition_columns(table.columns)
            if len(partition_cols) == 0:
                df = pl.scan_parquet(source=path)
            else:
                # TODO: There HAS to be a nicer way to do this. Try with Polars >1.0?
                df = pl.scan_parquet(
                    source=os.path.join(
                        path, *["**" for _ in range(len(partition_cols))], "*.parquet"
                    ),
                    hive_schema={
                        col.name: uc_type_to_polars_type(col.data_type)
                        for col in partition_cols
                    },
                )

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
            partition_cols = get_partition_columns(table.columns)
            if len(partition_cols) > 0:
                df.write_delta(
                    target=path,
                    mode="append",
                    delta_write_options={
                        "partition_by": [col.name for col in partition_cols]
                    },
                )
            else:
                df.write_delta(target=path, mode="append")
            return None

        case WriteMode.APPEND, FileType.DELTA, SchemaEvolution.UNION:
            raise NotImplementedError

        case WriteMode.APPEND, FileType.DELTA, SchemaEvolution.OVERWRITE:
            raise NotImplementedError

        case WriteMode.APPEND, FileType.PARQUET, SchemaEvolution.STRICT:
            partition_cols = get_partition_columns(table.columns)
            if len(partition_cols) == 0:
                raise UnsupportedOperationError(
                    "Appending is only supported for PARQUET when partitioned."
                )
            df_uc_schema = df_schema_to_uc_schema(df=df)
            if not check_schema_equality(left=df_uc_schema, right=table.columns):
                raise SchemaMismatchError(
                    f"Schema evolution is set to strict but schemas do not match: {df_uc_schema} VS {table.columns}"
                )
            df.write_parquet(
                file=path,
                use_pyarrow=True,
                pyarrow_options={
                    "partition_cols": [col.name for col in partition_cols],
                    "basename_template": str(uuid.uuid4())
                    + str(time.time()).replace(".", "")
                    + "-{i}.parquet",
                },
            )
            return None

        case WriteMode.APPEND, FileType.PARQUET, SchemaEvolution.UNION:
            raise NotImplementedError

        case WriteMode.APPEND, FileType.PARQUET, SchemaEvolution.OVERWRITE:
            raise NotImplementedError

        case WriteMode.APPEND, _, _:
            raise UnsupportedOperationError(
                f"Appending is not supported for {table.file_type.value}."
            )

        case WriteMode.OVERWRITE, FileType.DELTA, _:
            raise NotImplementedError

        case WriteMode.OVERWRITE, FileType.PARQUET, _:
            raise NotImplementedError

        case WriteMode.OVERWRITE, FileType.CSV, _:
            raise NotImplementedError

        case WriteMode.OVERWRITE, FileType.AVRO, _:
            raise NotImplementedError

        case _:
            raise NotImplementedError
