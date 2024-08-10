import polars as pl
import os
import time
import uuid
from enum import Enum
from typing import Literal, cast
from .exceptions import UnsupportedOperationError, SchemaMismatchError
from .models import Table, FileType, Column, DataType


class WriteMode(str, Enum):
    APPEND = "APPEND"
    OVERWRITE = "OVERWRITE"


class SchemaEvolution(str, Enum):
    STRICT = "STRICT"
    MERGE = "MERGE"
    OVERWRITE = "OVERWRITE"


def polars_type_to_uc_type(t: pl.DataType) -> tuple[DataType, int, int]:
    """
    Converts the enum DataType to a polars.DataType
    """
    match t:
        case pl.Decimal:
            return (DataType.DECIMAL, t.precision, t.scale)
        case pl.Float32:
            return (DataType.FLOAT, 0, 0)
        case pl.Float64:
            return (DataType.DOUBLE, 0, 0)
        case pl.Int8:
            return (DataType.BYTE, 0, 0)
        case pl.Int16:
            return (DataType.SHORT, 0, 0)
        case pl.Int32:
            return (DataType.INT, 0, 0)
        case pl.Int64:
            return (DataType.LONG, 0, 0)
        case pl.Date:
            return (DataType.DATE, 0, 0)
        case pl.Datetime:
            return (DataType.TIMESTAMP, 0, 0)
        case pl.Array:
            return (DataType.ARRAY, 0, 0)
        case pl.List:
            return (DataType.ARRAY, 0, 0)
        case pl.Struct:
            return (DataType.STRUCT, 0, 0)
        case pl.String | pl.Utf8:
            return (DataType.STRING, 0, 0)
        case pl.Binary:
            return (DataType.BINARY, 0, 0)
        case pl.Boolean:
            return (DataType.BOOLEAN, 0, 0)
        case pl.Null:
            return (DataType.NULL, 0, 0)
        case _:
            raise UnsupportedOperationError(f"Unsupported datatype: {t}")
    # Why did mypy start complaining about missing return here after bumping Polars to 1.3.0?
    return (DataType.NULL, 0, 0)


def df_schema_to_uc_schema(df: pl.DataFrame | pl.LazyFrame) -> list[Column]:
    res = []
    for i, (col_name, col_type) in enumerate(df.schema.items()):
        t = polars_type_to_uc_type(col_type)
        res.append(
            Column(
                name=col_name,
                data_type=t[0],
                type_precision=t[1],
                type_scale=t[2],
                position=i,
                nullable=True,
            )
        )
    return res


def uc_type_to_polars_type(
    t: DataType, precision: int = 0, scale: int = 0
) -> pl.DataType:
    match t:
        case DataType.BOOLEAN:
            return cast(pl.DataType, pl.Boolean)
        case DataType.BYTE:
            return cast(pl.DataType, pl.Int8)
        case DataType.SHORT:
            return cast(pl.DataType, pl.Int16)
        case DataType.INT:
            return cast(pl.DataType, pl.Int32)
        case DataType.LONG:
            return cast(pl.DataType, pl.Int64)
        case DataType.FLOAT:
            return cast(pl.DataType, pl.Float32)
        case DataType.DOUBLE:
            return cast(pl.DataType, pl.Float64)
        case DataType.DATE:
            return cast(pl.DataType, pl.Date)
        case DataType.TIMESTAMP:
            return cast(pl.DataType, pl.Datetime)
        case DataType.STRING:
            return cast(pl.DataType, pl.String)
        case DataType.BINARY:
            return cast(pl.DataType, pl.Binary)
        case DataType.DECIMAL:
            return cast(pl.DataType, pl.Decimal(precision=precision, scale=scale))
        case DataType.ARRAY:
            return cast(pl.DataType, pl.Array)
        case DataType.STRUCT:
            return cast(pl.DataType, pl.Struct)
        case DataType.CHAR:
            return cast(pl.DataType, pl.String)
        case DataType.NULL:
            return cast(pl.DataType, pl.Null)
        case _:
            raise UnsupportedOperationError(f"Unsupported datatype: {t.value}")


def uc_schema_to_df_schema(cols: list[Column]) -> dict[str, pl.DataType]:
    return {col.name: uc_type_to_polars_type(col.data_type) for col in cols}


def check_schema_equality(left: list[Column], right: list[Column]) -> bool:
    left = sorted(left, key=lambda x: x.position)
    right = sorted(right, key=lambda x: x.position)
    for left_col, right_col in zip(left, right):
        if left_col.name != right_col.name:
            return False
        if left_col.data_type != right_col.data_type:
            return False
        if left_col.data_type == DataType.DECIMAL and (
            left_col.type_precision != right_col.type_precision
            or left_col.type_scale != right_col.type_scale
        ):
            return False
    return True


def raise_for_schema_mismatch(
    df: pl.DataFrame | pl.LazyFrame, uc: list[Column]
) -> None:
    df_uc_schema = df_schema_to_uc_schema(df=df)
    if not check_schema_equality(left=df_uc_schema, right=uc):
        raise SchemaMismatchError(
            f"Schema evolution is set to strict but schemas do not match: {df_uc_schema} VS {uc}"
        )


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
                    hive_partitioning=True,
                    hive_schema={
                        col.name: uc_type_to_polars_type(col.data_type)
                        for col in partition_cols
                    },
                )

        case FileType.CSV:
            pl_schema = uc_schema_to_df_schema(table.columns)
            if len(pl_schema) == 0:
                df = pl.read_csv(source=path)
            else:
                df = pl.read_csv(source=path, schema=pl_schema)

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
                    hive_partitioning=True,
                    hive_schema={
                        col.name: uc_type_to_polars_type(col.data_type)
                        for col in partition_cols
                    },
                )

        case FileType.CSV:
            pl_schema = uc_schema_to_df_schema(table.columns)
            if len(pl_schema) == 0:
                df = pl.scan_csv(source=path)
            else:
                df = pl.scan_csv(source=path, schema=pl_schema)

        case FileType.AVRO:
            raise UnsupportedOperationError("scan is not supported for Avro.")

        case _:
            raise NotImplementedError

    return df


def write_table(
    table: Table,
    df: pl.DataFrame,
    mode: WriteMode,
    schema_evolution: SchemaEvolution,
) -> list[Column] | None:
    """
    Writes the Polars DataFrame `df` to the location of `table`.

    Returns None if the schema in Unity Catalog does NOT need to be updated.
    Returns a list[Column] if the schema in Unity Catalog DOES need to be updated.

    Raises UnsupportedOperationError for unsupported combination of `table.file_type`, `mode`, and `schema_evolution`.
    """
    path = table.storage_location
    assert path is not None
    if not path.startswith("file://"):
        raise UnsupportedOperationError("Only local storage is supported.")
    path = path.removeprefix("file://")

    match table.file_type, mode, schema_evolution:
        case FileType.DELTA, _, SchemaEvolution.STRICT:
            raise_for_schema_mismatch(df=df, uc=table.columns)
            partition_cols = get_partition_columns(table.columns)
            # needing to specify the cast is not neat,
            # but mypy gets angry if we just pass this as a str to write_delta
            write_mode = cast(Literal["append", "overwrite"], mode.value.lower())
            if len(partition_cols) > 0:
                df.write_delta(
                    target=path,
                    mode=write_mode,
                    delta_write_options={
                        "partition_by": [col.name for col in partition_cols]
                    },
                )
            else:
                df.write_delta(target=path, mode=write_mode)
            return None

        case FileType.DELTA, WriteMode.OVERWRITE, _:
            partition_cols = get_partition_columns(table.columns)
            # needing to specify the cast is not neat,
            # but mypy gets angry if we just pass this as a str to write_delta
            write_mode = cast(Literal["append", "overwrite"], mode.value.lower())
            if len(partition_cols) > 0:
                df.write_delta(
                    target=path,
                    mode=write_mode,
                    delta_write_options={
                        "partition_by": [col.name for col in partition_cols],
                        "schema_mode": "overwrite",
                    },
                )
            else:
                df.write_delta(
                    target=path,
                    mode=write_mode,
                    delta_write_options={"schema_mode": "overwrite"},
                )
            try:
                raise_for_schema_mismatch(df=df, uc=table.columns)
                return None
            except SchemaMismatchError:
                return df_schema_to_uc_schema(df=df)

        case FileType.DELTA, WriteMode.APPEND, SchemaEvolution.MERGE:
            raise NotImplementedError

        case FileType.PARQUET, WriteMode.APPEND, SchemaEvolution.STRICT:
            partition_cols = get_partition_columns(table.columns)
            if len(partition_cols) == 0:
                raise UnsupportedOperationError(
                    "Appending is only supported for PARQUET when partitioned."
                )
            raise_for_schema_mismatch(df=df, uc=table.columns)
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

        case FileType.PARQUET, WriteMode.OVERWRITE, _:
            if schema_evolution == SchemaEvolution.STRICT:
                raise_for_schema_mismatch(df=df, uc=table.columns)
            partition_cols = get_partition_columns(table.columns)
            if len(partition_cols) > 0:
                df.write_parquet(
                    file=path,
                    use_pyarrow=True,
                    pyarrow_options={
                        "partition_cols": [col.name for col in partition_cols],
                        "basename_template": str(uuid.uuid4())
                        + str(time.time()).replace(".", "")
                        + "-{i}.parquet",
                        "existing_data_behavior": "delete_matching",
                    },
                )
            else:
                df.write_parquet(file=path)
            try:
                raise_for_schema_mismatch(df=df, uc=table.columns)
                return None
            except SchemaMismatchError:
                return df_schema_to_uc_schema(df=df)

        case FileType.CSV, WriteMode.OVERWRITE, SchemaEvolution.STRICT:
            raise_for_schema_mismatch(df=df, uc=table.columns)
            df.write_csv(file=path)
            return None

        case FileType.CSV, WriteMode.OVERWRITE, SchemaEvolution.OVERWRITE:
            df.write_csv(file=path)
            try:
                raise_for_schema_mismatch(df=df, uc=table.columns)
                return None
            except SchemaMismatchError:
                return df_schema_to_uc_schema(df=df)

        case FileType.AVRO, WriteMode.OVERWRITE, SchemaEvolution.STRICT:
            raise_for_schema_mismatch(df=df, uc=table.columns)
            df.write_avro(file=path)
            return None

        case FileType.AVRO, WriteMode.OVERWRITE, SchemaEvolution.OVERWRITE:
            df.write_avro(file=path)
            try:
                raise_for_schema_mismatch(df=df, uc=table.columns)
                return None
            except SchemaMismatchError:
                return df_schema_to_uc_schema(df=df)

        case _, WriteMode.APPEND, _:
            raise UnsupportedOperationError(
                "Write mode APPEND is only supported for DELTA and partitioned PARQUET."
            )

        case _, _, SchemaEvolution.MERGE:
            raise UnsupportedOperationError(
                "Schema evolution MERGE is only supported for DELTA."
            )

        case _, _, SchemaEvolution.OVERWRITE:
            raise UnsupportedOperationError(
                "Schema evolution OVERWRITE is only supported when write mode is also OVERWRITE."
            )

        case _:
            raise UnsupportedOperationError(
                f"Unsupported parameters: {table.file_type}, {mode}, {schema_evolution}"
            )
