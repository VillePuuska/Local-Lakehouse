import os
import tempfile
import polars as pl
from polars.testing import assert_frame_equal
import random
import uuid
import string
import tempfile
import pytest
from uc_wrapper import (
    UCClient,
    Column,
    Table,
    TableType,
    FileType,
    DataType,
    WriteMode,
    SchemaEvolution,
    SchemaMismatchError,
)


RANDOM_DF_ROWS = 10


def random_df() -> pl.DataFrame:
    uuids = [str(uuid.uuid4()) for _ in range(RANDOM_DF_ROWS)]
    ints = [random.randint(0, 10000) for _ in range(RANDOM_DF_ROWS)]
    floats = [random.uniform(0, 10000) for _ in range(RANDOM_DF_ROWS)]
    strings = [
        "".join(
            random.choices(population=string.ascii_letters, k=random.randint(2, 256))
        )
        for _ in range(RANDOM_DF_ROWS)
    ]
    return pl.DataFrame(
        {
            "id": uuids,
            "ints": ints,
            "floats": floats,
            "strings": strings,
        },
        schema={
            "id": pl.String,
            "ints": pl.Int64,
            "floats": pl.Float64,
            "strings": pl.String,
        },
    )


def random_partitioned_df() -> pl.DataFrame:
    df = random_df()
    part1 = random.choices(population=[0, 1, 2], k=df.height)
    part2 = random.choices(population=[0, 1, 2], k=df.height)
    df = df.with_columns(
        [pl.Series(part1).alias("part1"), pl.Series(part2).alias("part2")]
    )
    return df


@pytest.mark.parametrize(
    "filetype",
    [
        FileType.DELTA,
        FileType.PARQUET,
        FileType.CSV,
        FileType.AVRO,
    ],
)
def test_basic_dataframe_operations(client: UCClient, filetype: FileType):
    assert client.health_check()

    default_catalog = "unity"
    default_schema = "default"
    table_name = "test_table"

    with tempfile.TemporaryDirectory() as tmpdir:
        match filetype:
            case FileType.DELTA:
                filepath = tmpdir
            case FileType.PARQUET:
                filepath = os.path.join(tmpdir, table_name + ".parquet")
            case FileType.CSV:
                filepath = os.path.join(tmpdir, table_name + ".csv")
            case FileType.AVRO:
                filepath = os.path.join(tmpdir, table_name + ".avro")
            case _:
                raise NotImplementedError
        columns = [
            Column(
                name="id",
                data_type=DataType.STRING,
                position=0,
                nullable=False,
            ),
            Column(
                name="ints",
                data_type=DataType.LONG,
                position=1,
                nullable=False,
            ),
            Column(
                name="floats",
                data_type=DataType.DOUBLE,
                position=2,
                nullable=False,
            ),
            Column(
                name="strings",
                data_type=DataType.STRING,
                position=3,
                nullable=False,
            ),
        ]
        client.create_table(
            Table(
                name=table_name,
                catalog_name=default_catalog,
                schema_name=default_schema,
                table_type=TableType.EXTERNAL,
                file_type=filetype,
                columns=columns,
                storage_location=filepath,
            )
        )

        df = random_df()

        client.write_table(
            df,
            catalog=default_catalog,
            schema=default_schema,
            name=table_name,
            mode=WriteMode.OVERWRITE,
            schema_evolution=SchemaEvolution.STRICT,
        )

        # Test read_table and scan_table
        df_read = client.read_table(
            catalog=default_catalog, schema=default_schema, name=table_name
        )
        assert_frame_equal(df, df_read, check_row_order=False)

        if filetype != FileType.AVRO:
            df_scan = client.scan_table(
                catalog=default_catalog, schema=default_schema, name=table_name
            )
            assert_frame_equal(pl.LazyFrame(df), df_scan, check_row_order=False)

        # Test APPEND writes; only supported for DELTA
        if filetype == FileType.DELTA:
            df2 = random_df()
            client.write_table(
                df2,
                catalog=default_catalog,
                schema=default_schema,
                name=table_name,
                mode=WriteMode.APPEND,
                schema_evolution=SchemaEvolution.STRICT,
            )
            df_read2 = client.read_table(
                catalog=default_catalog, schema=default_schema, name=table_name
            )
            assert_frame_equal(pl.concat([df, df2]), df_read2, check_row_order=False)
            assert_frame_equal(
                pl.concat([df, df2]),
                pl.read_delta(source=filepath),
                check_row_order=False,
            )

            with pytest.raises(SchemaMismatchError):
                df3 = random_df()
                df3 = df3.cast({"ints": pl.String})
                client.write_table(
                    df3,
                    catalog=default_catalog,
                    schema=default_schema,
                    name=table_name,
                    mode=WriteMode.APPEND,
                    schema_evolution=SchemaEvolution.STRICT,
                )

        df4 = random_df()

        # Test OVERWRITE writes
        client.write_table(
            df4,
            catalog=default_catalog,
            schema=default_schema,
            name=table_name,
            mode=WriteMode.OVERWRITE,
            schema_evolution=SchemaEvolution.STRICT,
        )

        assert_frame_equal(
            df4,
            client.read_table(
                catalog=default_catalog, schema=default_schema, name=table_name
            ),
            check_row_order=False,
        )

        if filetype != FileType.AVRO:
            df4_scan = client.scan_table(
                catalog=default_catalog, schema=default_schema, name=table_name
            )
            assert_frame_equal(pl.LazyFrame(df4), df4_scan, check_row_order=False)


@pytest.mark.parametrize(
    "filetype",
    [
        FileType.DELTA,
        FileType.PARQUET,
    ],
)
def test_partitioned_dataframe_operations(client: UCClient, filetype: FileType):
    assert client.health_check()

    default_catalog = "unity"
    default_schema = "default"
    table_name = "test_table"

    with tempfile.TemporaryDirectory() as tmpdir:
        match filetype:
            case FileType.DELTA:
                filepath = tmpdir
            case FileType.PARQUET:
                filepath = tmpdir
            case _:
                raise NotImplementedError
        columns = [
            Column(
                name="id",
                data_type=DataType.STRING,
                position=0,
                nullable=False,
            ),
            Column(
                name="ints",
                data_type=DataType.LONG,
                position=1,
                nullable=False,
            ),
            Column(
                name="floats",
                data_type=DataType.DOUBLE,
                position=2,
                nullable=False,
            ),
            Column(
                name="strings",
                data_type=DataType.STRING,
                position=3,
                nullable=False,
            ),
            Column(
                name="part1",
                data_type=DataType.LONG,
                position=4,
                nullable=False,
                partition_index=0,
            ),
            Column(
                name="part2",
                data_type=DataType.LONG,
                position=5,
                nullable=False,
                partition_index=1,
            ),
        ]
        client.create_table(
            Table(
                name=table_name,
                catalog_name=default_catalog,
                schema_name=default_schema,
                table_type=TableType.EXTERNAL,
                file_type=filetype,
                columns=columns,
                storage_location=filepath,
            )
        )

        df = random_partitioned_df()

        client.write_table(
            df,
            catalog=default_catalog,
            schema=default_schema,
            name=table_name,
            mode=WriteMode.OVERWRITE,
            schema_evolution=SchemaEvolution.STRICT,
        )

        # Test read and scan
        df_read = client.read_table(
            catalog=default_catalog, schema=default_schema, name=table_name
        )
        assert_frame_equal(df, df_read, check_row_order=False)

        df_scan = client.scan_table(
            catalog=default_catalog, schema=default_schema, name=table_name
        )
        assert_frame_equal(pl.LazyFrame(df), df_scan, check_row_order=False)

        # Test APPEND writes
        df2 = random_partitioned_df()
        client.write_table(
            df2,
            catalog=default_catalog,
            schema=default_schema,
            name=table_name,
            mode=WriteMode.APPEND,
            schema_evolution=SchemaEvolution.STRICT,
        )
        df_read2 = client.read_table(
            catalog=default_catalog, schema=default_schema, name=table_name
        )
        assert_frame_equal(pl.concat([df, df2]), df_read2, check_row_order=False)
        if filetype == FileType.DELTA:
            assert_frame_equal(
                pl.concat([df, df2]),
                pl.read_delta(source=filepath),
                check_row_order=False,
            )
        if filetype == FileType.PARQUET:
            assert_frame_equal(
                pl.concat([df, df2]),
                pl.read_parquet(
                    source=os.path.join(filepath, "**", "**", "*.parquet"),
                    hive_partitioning=True,
                    hive_schema={"part1": pl.Int64, "part2": pl.Int64},
                ),
                check_row_order=False,
            )

        with pytest.raises(SchemaMismatchError):
            df3 = random_df()
            df3 = df3.cast({"ints": pl.String})
            client.write_table(
                df3,
                catalog=default_catalog,
                schema=default_schema,
                name=table_name,
                mode=WriteMode.APPEND,
                schema_evolution=SchemaEvolution.STRICT,
            )

        df4 = pl.concat([random_partitioned_df(), random_partitioned_df()])
        # This rewriting of part1 and part2 guarantees that we will overwrite
        # every partition. Otherwise, we might not overwrite all data
        # in the case of a partitioned Parquet table.
        df_concat = pl.concat([df, df2])
        df4 = df4.replace_column(4, df_concat.select("part1").to_series())
        df4 = df4.replace_column(5, df_concat.select("part2").to_series())

        # Test OVERWRITE writes
        client.write_table(
            df4,
            catalog=default_catalog,
            schema=default_schema,
            name=table_name,
            mode=WriteMode.OVERWRITE,
            schema_evolution=SchemaEvolution.STRICT,
        )

        assert_frame_equal(
            df4,
            client.read_table(
                catalog=default_catalog, schema=default_schema, name=table_name
            ),
            check_row_order=False,
        )

        df4_scan = client.scan_table(
            catalog=default_catalog, schema=default_schema, name=table_name
        )
        assert_frame_equal(pl.LazyFrame(df4), df4_scan, check_row_order=False)
