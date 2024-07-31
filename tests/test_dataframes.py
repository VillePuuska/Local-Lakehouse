import os
import tempfile
import polars as pl
from polars.testing import assert_frame_equal, assert_frame_not_equal
import deltalake
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
    "file_type",
    [
        FileType.DELTA,
        FileType.PARQUET,
        FileType.CSV,
        FileType.AVRO,
    ],
)
def test_basic_dataframe_operations(client: UCClient, file_type: FileType):
    assert client.health_check()

    default_catalog = "unity"
    default_schema = "default"
    table_name = "test_table"

    with tempfile.TemporaryDirectory() as tmpdir:
        match file_type:
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
                file_type=file_type,
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
            mode="overwrite",
            schema_evolution="strict",
        )

        # Test read_table and scan_table
        df_read = client.read_table(
            catalog=default_catalog, schema=default_schema, name=table_name
        )
        assert_frame_equal(df, df_read, check_row_order=False)

        if file_type != FileType.AVRO:
            df_scan = client.scan_table(
                catalog=default_catalog, schema=default_schema, name=table_name
            )
            assert_frame_equal(pl.LazyFrame(df), df_scan, check_row_order=False)

        # Test APPEND writes; only supported for DELTA
        if file_type == FileType.DELTA:
            df2 = random_df()
            client.write_table(
                df2,
                catalog=default_catalog,
                schema=default_schema,
                name=table_name,
                mode="append",
                schema_evolution="strict",
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
                    mode="append",
                    schema_evolution="strict",
                )

        df4 = random_df()

        # Test OVERWRITE writes
        client.write_table(
            df4,
            catalog=default_catalog,
            schema=default_schema,
            name=table_name,
            mode="overwrite",
            schema_evolution="strict",
        )

        assert_frame_equal(
            df4,
            client.read_table(
                catalog=default_catalog, schema=default_schema, name=table_name
            ),
            check_row_order=False,
        )

        if file_type != FileType.AVRO:
            df4_scan = client.scan_table(
                catalog=default_catalog, schema=default_schema, name=table_name
            )
            assert_frame_equal(pl.LazyFrame(df4), df4_scan, check_row_order=False)

        df5 = random_df()
        df5 = df5.cast({"ints": pl.String})

        table = client.get_table(
            catalog=default_catalog, schema=default_schema, table=table_name
        )
        modified_col = [col for col in table.columns if col.name == "ints"][0]
        assert modified_col.data_type == DataType.LONG
        assert modified_col.position == 1

        client.write_table(
            df5,
            catalog=default_catalog,
            schema=default_schema,
            name=table_name,
            mode="overwrite",
            schema_evolution="overwrite",
        )

        table = client.get_table(
            catalog=default_catalog, schema=default_schema, table=table_name
        )
        modified_col = [col for col in table.columns if col.name == "ints"][0]
        assert modified_col.data_type == DataType.STRING
        assert modified_col.position == 1

        assert_frame_equal(
            df5,
            client.read_table(
                catalog=default_catalog, schema=default_schema, name=table_name
            ),
            check_row_order=False,
        )

        if file_type != FileType.AVRO:
            df5_scan = client.scan_table(
                catalog=default_catalog, schema=default_schema, name=table_name
            )
            assert_frame_equal(pl.LazyFrame(df5), df5_scan, check_row_order=False)


@pytest.mark.parametrize(
    "file_type",
    [
        FileType.DELTA,
        FileType.PARQUET,
    ],
)
def test_partitioned_dataframe_operations(client: UCClient, file_type: FileType):
    assert client.health_check()

    default_catalog = "unity"
    default_schema = "default"
    table_name = "test_table"

    with tempfile.TemporaryDirectory() as tmpdir:
        match file_type:
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
                file_type=file_type,
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
            mode="overwrite",
            schema_evolution="strict",
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
            mode="append",
            schema_evolution="strict",
        )
        df_read2 = client.read_table(
            catalog=default_catalog, schema=default_schema, name=table_name
        )
        assert_frame_equal(pl.concat([df, df2]), df_read2, check_row_order=False)
        if file_type == FileType.DELTA:
            assert_frame_equal(
                pl.concat([df, df2]),
                pl.read_delta(source=filepath),
                check_row_order=False,
            )
        if file_type == FileType.PARQUET:
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
                mode="append",
                schema_evolution="strict",
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
            mode="overwrite",
            schema_evolution="strict",
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

        df5 = pl.concat([random_partitioned_df(), random_partitioned_df()])
        df5 = df5.cast({"ints": pl.String})
        df5 = df5.replace_column(4, df_concat.select("part1").to_series())
        df5 = df5.replace_column(5, df_concat.select("part2").to_series())

        table = client.get_table(
            catalog=default_catalog, schema=default_schema, table=table_name
        )
        modified_col = [col for col in table.columns if col.name == "ints"][0]
        assert modified_col.data_type == DataType.LONG
        assert modified_col.position == 1

        client.write_table(
            df5,
            catalog=default_catalog,
            schema=default_schema,
            name=table_name,
            mode="overwrite",
            schema_evolution="overwrite",
        )

        table = client.get_table(
            catalog=default_catalog, schema=default_schema, table=table_name
        )
        modified_col = [col for col in table.columns if col.name == "ints"][0]
        assert modified_col.data_type == DataType.STRING
        assert modified_col.position == 1

        assert_frame_equal(
            df5,
            client.read_table(
                catalog=default_catalog, schema=default_schema, name=table_name
            ),
            check_row_order=False,
        )

        df5_scan = client.scan_table(
            catalog=default_catalog, schema=default_schema, name=table_name
        )
        assert_frame_equal(pl.LazyFrame(df5), df5_scan, check_row_order=False)


@pytest.mark.parametrize(
    "file_type,partitioned",
    [
        (FileType.DELTA, False),
        (FileType.PARQUET, False),
        (FileType.DELTA, True),
        (FileType.PARQUET, True),
        (FileType.CSV, False),
        (FileType.AVRO, False),
    ],
)
def test_create_as_table(client: UCClient, file_type: FileType, partitioned: bool):
    assert client.health_check()

    default_catalog = "unity"
    default_schema = "default"
    table_name = "test_table"

    with tempfile.TemporaryDirectory() as tmpdir:
        match file_type:
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

        if not partitioned:
            df = random_df()
            client.create_as_table(
                df=df,
                catalog=default_catalog,
                schema=default_schema,
                name=table_name,
                file_type=file_type,
                table_type="external",
                location="file://" + filepath,
            )
        else:
            df = random_partitioned_df()
            client.create_as_table(
                df=df,
                catalog=default_catalog,
                schema=default_schema,
                name=table_name,
                file_type=file_type.value.lower(),  # test this works with string literal as well
                table_type="external",
                location="file://" + filepath,
                partition_cols=["part1", "part2"],
            )

            # Test the written table is actually partitioned
            if file_type == FileType.DELTA:
                tbl_read = deltalake.DeltaTable(table_uri=filepath)
                assert tbl_read.metadata().partition_columns == ["part1", "part2"]
            elif file_type == FileType.PARQUET:
                df_read = pl.read_parquet(
                    source=os.path.join(filepath, "**", "**", "*.parquet"),
                    hive_partitioning=True,
                    hive_schema={"part1": pl.Int64, "part2": pl.Int64},
                )
                assert_frame_equal(df, df_read, check_row_order=False)
                assert_frame_not_equal(
                    df,
                    pl.read_parquet(source=filepath, hive_partitioning=False),
                    check_row_order=False,
                )

        df_read = client.read_table(
            catalog=default_catalog, schema=default_schema, name=table_name
        )
        assert_frame_equal(df, df_read, check_row_order=False)


def test_register_as_table(client: UCClient):
    assert client.health_check()

    default_catalog = "unity"
    default_schema = "default"
    table_name = "test_table"

    # Non-partitioned delta
    with tempfile.TemporaryDirectory() as tmpdir:
        df = random_df()
        df.write_delta(target=tmpdir)

        client.register_as_table(
            filepath=tmpdir,
            catalog=default_catalog,
            schema=default_schema,
            name=table_name + "np-delta",
            file_type="delta",
        )
        table = client.get_table(
            catalog=default_catalog,
            schema=default_schema,
            table=table_name + "np-delta",
        )
        assert (
            len([col for col in table.columns if col.partition_index is not None]) == 0
        )

        df_read = client.read_table(
            catalog=default_catalog, schema=default_schema, name=table_name + "np-delta"
        )
        assert_frame_equal(df, df_read, check_row_order=False)

    # Partitioned delta
    with tempfile.TemporaryDirectory() as tmpdir:
        df = random_partitioned_df()
        df.write_delta(
            target=tmpdir, delta_write_options={"partition_by": ["part1", "part2"]}
        )

        client.register_as_table(
            filepath=tmpdir,
            catalog=default_catalog,
            schema=default_schema,
            name=table_name + "p-delta",
            file_type="delta",
            partition_cols=["part1", "part2"],
        )
        table = client.get_table(
            catalog=default_catalog, schema=default_schema, table=table_name + "p-delta"
        )
        assert (
            len([col for col in table.columns if col.partition_index is not None]) == 2
        )

        df_read = client.read_table(
            catalog=default_catalog, schema=default_schema, name=table_name + "p-delta"
        )
        assert_frame_equal(df, df_read, check_row_order=False)

    # Non-partitioned parquet
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "safvlsdv.parquet")
        df = random_df()
        df.write_parquet(file=filepath)

        client.register_as_table(
            filepath=filepath,
            catalog=default_catalog,
            schema=default_schema,
            name=table_name + "np-parquet",
            file_type="parquet",
        )
        table = client.get_table(
            catalog=default_catalog,
            schema=default_schema,
            table=table_name + "np-parquet",
        )
        assert (
            len([col for col in table.columns if col.partition_index is not None]) == 0
        )

        df_read = client.read_table(
            catalog=default_catalog,
            schema=default_schema,
            name=table_name + "np-parquet",
        )
        assert_frame_equal(df, df_read, check_row_order=False)

    # Partitioned delta
    with tempfile.TemporaryDirectory() as tmpdir:
        df = random_partitioned_df()
        df.write_parquet(
            file=tmpdir,
            use_pyarrow=True,
            pyarrow_options={
                "partition_cols": ["part1", "part2"],
            },
        )

        client.register_as_table(
            filepath=tmpdir,
            catalog=default_catalog,
            schema=default_schema,
            name=table_name + "p-parquet",
            file_type="parquet",
            partition_cols=["part1", "part2"],
        )
        table = client.get_table(
            catalog=default_catalog,
            schema=default_schema,
            table=table_name + "p-parquet",
        )
        assert (
            len([col for col in table.columns if col.partition_index is not None]) == 2
        )

        df_read = client.read_table(
            catalog=default_catalog,
            schema=default_schema,
            name=table_name + "p-parquet",
        )
        assert_frame_equal(df, df_read, check_row_order=False)

    # CSV
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "sgvsavdavsdsvd.csv")
        df = random_df()
        df.write_csv(file=filepath)

        client.register_as_table(
            filepath=filepath,
            catalog=default_catalog,
            schema=default_schema,
            name=table_name + "csv",
            file_type="csv",
        )
        table = client.get_table(
            catalog=default_catalog,
            schema=default_schema,
            table=table_name + "csv",
        )
        assert (
            len([col for col in table.columns if col.partition_index is not None]) == 0
        )

        df_read = client.read_table(
            catalog=default_catalog,
            schema=default_schema,
            name=table_name + "csv",
        )
        assert_frame_equal(df, df_read, check_row_order=False)

    # AVRO
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "iuaevbaerv.avro")
        df = random_df()
        df.write_avro(file=filepath)

        client.register_as_table(
            filepath=filepath,
            catalog=default_catalog,
            schema=default_schema,
            name=table_name + "avro",
            file_type="avro",
        )
        table = client.get_table(
            catalog=default_catalog,
            schema=default_schema,
            table=table_name + "avro",
        )
        assert (
            len([col for col in table.columns if col.partition_index is not None]) == 0
        )

        df_read = client.read_table(
            catalog=default_catalog,
            schema=default_schema,
            name=table_name + "avro",
        )
        assert_frame_equal(df, df_read, check_row_order=False)
