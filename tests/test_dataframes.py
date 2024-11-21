import os
import tempfile
import polars as pl
from polars.testing import assert_frame_equal, assert_frame_not_equal
import deltalake
import tempfile
import pytest
from typing import Callable, Literal, cast
from uchelper import (
    UCClient,
    Column,
    Table,
    TableType,
    FileType,
    DataType,
    SchemaMismatchError,
)


@pytest.mark.parametrize(
    "file_type",
    [
        FileType.DELTA,
        FileType.PARQUET,
        FileType.CSV,
        FileType.AVRO,
    ],
)
def test_basic_dataframe_operations(
    client: UCClient,
    random_df: Callable[[], pl.DataFrame],
    random_df_cols: list[Column],
    file_type: FileType,
):
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

        # Polars does not support DECIMAL when reading CSVs
        if file_type == FileType.CSV:
            random_df_cols[3].data_type = DataType.DOUBLE
            random_df_cols[3].type_precision = 0
            random_df_cols[3].type_scale = 0

        client.create_table(
            Table(
                name=table_name,
                catalog_name=default_catalog,
                schema_name=default_schema,
                table_type=TableType.EXTERNAL,
                file_type=file_type,
                columns=random_df_cols,
                storage_location=filepath,
            )
        )

        df = random_df()
        # Polars does not support DECIMAL when reading CSVs
        if file_type == FileType.CSV:
            df = df.cast({"decimals": pl.Float64})

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
        # Polars does not support DECIMAL when reading CSVs
        if file_type == FileType.CSV:
            df4 = df4.cast({"decimals": pl.Float64})

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
        # Polars does not support DECIMAL when reading CSVs
        if file_type == FileType.CSV:
            df5 = df5.cast({"decimals": pl.Float64})

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
def test_partitioned_dataframe_operations(
    client: UCClient,
    random_df: Callable[[], pl.DataFrame],
    random_partitioned_df: Callable[[], pl.DataFrame],
    random_partitioned_df_cols: list[Column],
    file_type: FileType,
):
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

        client.create_table(
            Table(
                name=table_name,
                catalog_name=default_catalog,
                schema_name=default_schema,
                table_type=TableType.EXTERNAL,
                file_type=file_type,
                columns=random_partitioned_df_cols,
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
        df4 = df4.replace_column(5, df_concat.select("part1").to_series())
        df4 = df4.replace_column(6, df_concat.select("part2").to_series())

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
        df5 = df5.replace_column(5, df_concat.select("part1").to_series())
        df5 = df5.replace_column(6, df_concat.select("part2").to_series())

        table = client.get_table(
            catalog=default_catalog, schema=default_schema, table=table_name
        )
        modified_col = [col for col in table.columns if col.name == "ints"][0]
        assert modified_col.data_type == DataType.LONG
        assert modified_col.position == 1

        partition_cols = sorted(
            [
                (col.partition_index, col.name)
                for col in table.columns
                if col.partition_index is not None
            ]
        )
        assert partition_cols == [(0, "part1"), (1, "part2")]

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

        partition_cols = sorted(
            [
                (col.partition_index, col.name)
                for col in table.columns
                if col.partition_index is not None
            ]
        )
        assert partition_cols == [(0, "part1"), (1, "part2")]

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
def test_create_as_table(
    client: UCClient,
    random_df: Callable[[], pl.DataFrame],
    random_partitioned_df: Callable[[], pl.DataFrame],
    file_type: FileType,
    partitioned: bool,
):
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
            # Polars does not support DECIMAL when reading CSVs
            if file_type == FileType.CSV:
                df = df.cast({"decimals": pl.Float64})

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
            ft = cast(Literal["delta", "parquet"], file_type.value.lower())
            assert ft in ["delta", "parquet"]

            df = random_partitioned_df()
            client.create_as_table(
                df=df,
                catalog=default_catalog,
                schema=default_schema,
                name=table_name,
                file_type=ft,  # test this works with string literal as well
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
def test_register_as_table(
    client: UCClient,
    random_df: Callable[[], pl.DataFrame],
    random_partitioned_df: Callable[[], pl.DataFrame],
    file_type: FileType,
    partitioned: bool,
):
    assert client.health_check()

    default_catalog = "unity"
    default_schema = "default"
    table_name = "test_table"

    with tempfile.TemporaryDirectory() as tmpdir:
        match file_type, partitioned:
            case FileType.DELTA, False:
                filepath = tmpdir
                df = random_df()
                df.write_delta(target=tmpdir)
            case FileType.DELTA, True:
                filepath = tmpdir
                df = random_partitioned_df()
                df.write_delta(
                    target=tmpdir,
                    delta_write_options={"partition_by": ["part1", "part2"]},
                )
            case FileType.PARQUET, False:
                filepath = os.path.join(tmpdir, "safvlsdv.parquet")
                df = random_df()
                df.write_parquet(file=filepath)
            case FileType.PARQUET, True:
                filepath = tmpdir
                df = random_partitioned_df()
                df.write_parquet(
                    file=tmpdir,
                    use_pyarrow=True,
                    pyarrow_options={
                        "partition_cols": ["part1", "part2"],
                    },
                )
            case FileType.CSV, False:
                filepath = os.path.join(tmpdir, "sgvsavdavsdsvd.csv")
                df = random_df().cast({"decimals": pl.Float64})
                df.write_csv(file=filepath)
            case FileType.AVRO, False:
                filepath = os.path.join(tmpdir, "iuaevbaerv.avro")
                df = random_df()
                df.write_avro(file=filepath)
            case _:
                raise Exception("Impossible codepath")

        client.register_as_table(
            filepath=filepath,
            catalog=default_catalog,
            schema=default_schema,
            name=table_name,
            file_type=file_type,
            partition_cols=None if not partitioned else ["part1", "part2"],
        )

        table = client.get_table(
            catalog=default_catalog,
            schema=default_schema,
            table=table_name,
        )
        assert (
            len([col for col in table.columns if col.partition_index is not None]) == 0
            if not partitioned
            else 2
        )

        df_read = client.read_table(
            catalog=default_catalog, schema=default_schema, name=table_name
        )
        assert_frame_equal(df, df_read, check_row_order=False)


@pytest.mark.parametrize(
    "partitioned",
    [True, False],
)
def test_write_delta_table_merge_schema(
    client: UCClient,
    random_df: Callable[[], pl.DataFrame],
    random_df_cols: list[Column],
    random_partitioned_df: Callable[[], pl.DataFrame],
    random_partitioned_df_cols: list[Column],
    partitioned: bool,
):
    assert client.health_check()

    default_catalog = "unity"
    default_schema = "default"
    table_name = "test_table"

    with tempfile.TemporaryDirectory() as tmpdir:
        if not partitioned:
            df = random_df()
            df2 = (
                random_df()
                .cast({"floats": pl.String})
                .rename({"floats": "more_strings"})
            )
            partition_cols = None
            cols = random_df_cols
        else:
            df = random_partitioned_df()
            df2 = (
                random_partitioned_df()
                .cast({"floats": pl.String})
                .rename({"floats": "more_strings"})
            )
            partition_cols = ["part1", "part2"]
            cols = random_partitioned_df_cols

        client.create_as_table(
            df=df,
            catalog=default_catalog,
            schema=default_schema,
            name=table_name,
            file_type="delta",
            table_type="external",
            location="file://" + tmpdir,
            partition_cols=partition_cols,
        )

        client.write_table(
            df=df2,
            catalog=default_catalog,
            schema=default_schema,
            name=table_name,
            mode="append",
            schema_evolution="merge",
        )

        df_read = client.read_table(
            catalog=default_catalog, schema=default_schema, name=table_name
        )
        assert_frame_equal(
            pl.concat([df, df2], how="diagonal"),
            df_read,
            check_column_order=False,
            check_row_order=False,
        )

        table = client.get_table(
            catalog=default_catalog, schema=default_schema, table=table_name
        )
        assert set(
            (col.name, col.data_type, col.partition_index) for col in cols
        ).union(set([("more_strings", DataType.STRING, None)])) == set(
            (col.name, col.data_type, col.partition_index) for col in table.columns
        )


def test_merge_table(client: UCClient):
    assert client.health_check()

    default_catalog = "unity"
    default_schema = "default"
    table_name = "merge_test_table"

    df1 = pl.DataFrame(
        data={
            "id": [0, 1, 2, 3, 4, 5],
            "str_col": ["asd", "foo", "bar", "baz", "tmp", "mic check 123"],
            "float_col": [0.1, 1.2, 3.4, 99.99, 123.321, -12.32],
        },
        schema={
            "id": pl.Int64,
            "str_col": pl.String,
            "float_col": pl.Float64,
        },
    )
    df2 = pl.DataFrame(
        data={
            "id": [0, 1, 6, 7],
            "str_col": ["asddd", "foo", "bar", "baz"],
            "float_col": [0.1, 11.2, 1.1, 2.2],
        },
        schema={
            "id": pl.Int64,
            "str_col": pl.String,
            "float_col": pl.Float64,
        },
    )
    df3 = pl.DataFrame(
        data={
            "id": [0, 1, 6, 7],
            "str_col": ["hi", "mom", "bar", "baz"],
            "float_col": [0.1, 11.2, 0.0, 0.0],
        },
        schema={
            "id": pl.Int64,
            "str_col": pl.String,
            "float_col": pl.Float64,
        },
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        client.create_as_table(
            df=df1,
            catalog=default_catalog,
            schema=default_schema,
            name=table_name,
            file_type="delta",
            table_type="external",
            location="file://" + tmpdir,
        )
        assert_frame_equal(
            df1,
            client.read_table(
                catalog=default_catalog, schema=default_schema, name=table_name
            ),
            check_row_order=False,
        )

        with pytest.raises(Exception):
            client.merge_table(
                df=df2, catalog=default_catalog, schema=default_schema, name=table_name
            )

        client.set_table_default_merge_columns(
            catalog=default_catalog,
            schema=default_schema,
            table=table_name,
            merge_columns=["id"],
        )
        client.merge_table(
            df=df2, catalog=default_catalog, schema=default_schema, name=table_name
        ).when_not_matched_insert_all().execute()

        result_first_merge = pl.DataFrame(
            data={
                "id": [0, 1, 2, 3, 4, 5, 6, 7],
                "str_col": [
                    "asd",
                    "foo",
                    "bar",
                    "baz",
                    "tmp",
                    "mic check 123",
                    "bar",
                    "baz",
                ],
                "float_col": [0.1, 1.2, 3.4, 99.99, 123.321, -12.32, 1.1, 2.2],
            },
            schema={
                "id": pl.Int64,
                "str_col": pl.String,
                "float_col": pl.Float64,
            },
        )
        assert_frame_equal(
            result_first_merge,
            client.read_table(
                catalog=default_catalog, schema=default_schema, name=table_name
            ),
            check_row_order=False,
        )

        client.set_table_default_merge_columns(
            catalog=default_catalog,
            schema=default_schema,
            table=table_name,
            merge_columns=["id", "str_col"],
        )
        client.merge_table(
            df=df3, catalog=default_catalog, schema=default_schema, name=table_name
        ).when_matched_update_all().when_not_matched_by_source_update(
            updates={"float_col": "-111.0"}
        ).execute()

        result_second_merge = pl.DataFrame(
            data={
                "id": [0, 1, 2, 3, 4, 5, 6, 7],
                "str_col": [
                    "asd",
                    "foo",
                    "bar",
                    "baz",
                    "tmp",
                    "mic check 123",
                    "bar",
                    "baz",
                ],
                "float_col": [-111.0, -111.0, -111.0, -111.0, -111.0, -111.0, 0.0, 0.0],
            },
            schema={
                "id": pl.Int64,
                "str_col": pl.String,
                "float_col": pl.Float64,
            },
        )
        assert_frame_equal(
            result_second_merge,
            client.read_table(
                catalog=default_catalog, schema=default_schema, name=table_name
            ),
            check_row_order=False,
        )

        client.merge_table(
            df=df3,
            catalog=default_catalog,
            schema=default_schema,
            name=table_name,
            merge_condition="s.id = t.id",
            source_alias="s",
            target_alias="t",
        ).when_not_matched_by_source_delete().execute()
        result_third_merge = pl.DataFrame(
            data={
                "id": [0, 1, 6, 7],
                "str_col": [
                    "asd",
                    "foo",
                    "bar",
                    "baz",
                ],
                "float_col": [-111.0, -111.0, 0.0, 0.0],
            },
            schema={
                "id": pl.Int64,
                "str_col": pl.String,
                "float_col": pl.Float64,
            },
        )
        assert_frame_equal(
            result_third_merge,
            client.read_table(
                catalog=default_catalog, schema=default_schema, name=table_name
            ),
            check_row_order=False,
        )


@pytest.mark.parametrize(
    "partitioned",
    [True, False],
)
def test_replace_where(
    client: UCClient,
    partitioned: bool,
):
    assert client.health_check()

    default_catalog = "unity"
    default_schema = "default"
    table_name = "test_table"

    df1 = pl.DataFrame(
        data={
            "id": [0, 1, 2, 3, 4, 5],
            "str_col": ["asd", "foo", "bar", "baz", "tmp", "mic check 123"],
            "float_col": [0.1, 1.2, 3.4, 99.99, 123.321, -12.32],
        },
        schema={
            "id": pl.Int64,
            "str_col": pl.String,
            "float_col": pl.Float64,
        },
    )
    df2 = pl.DataFrame(
        data={
            "id": [0, 0, 0, 0],
            "str_col": ["asddd", "foo", "bar", "baz"],
            "float_col": [0.1, 11.2, 1.1, 2.2],
        },
        schema={
            "id": pl.Int64,
            "str_col": pl.String,
            "float_col": pl.Float64,
        },
    )
    df3 = pl.DataFrame(
        data={
            "id": [6, 7],
            "str_col": ["hiiiii", "mommmm"],
            "float_col": [0.1, 11.2],
        },
        schema={
            "id": pl.Int64,
            "str_col": pl.String,
            "float_col": pl.Float64,
        },
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        if not partitioned:
            client.create_as_table(
                df=df1,
                catalog=default_catalog,
                schema=default_schema,
                name=table_name,
                file_type="delta",
                table_type="external",
                location="file://" + tmpdir,
            )
        else:
            client.create_as_table(
                df=df1,
                catalog=default_catalog,
                schema=default_schema,
                name=table_name,
                file_type="delta",
                table_type="external",
                location="file://" + tmpdir,
                partition_cols=["id"],
            )

        df = client.read_table(
            catalog=default_catalog,
            schema=default_schema,
            name=table_name,
        )
        assert_frame_equal(df, df1, check_row_order=False)

        client.write_table(
            df=df2,
            catalog=default_catalog,
            schema=default_schema,
            name=table_name,
            mode="overwrite",
            replace_where="id = 0",
        )
        expected = pl.DataFrame(
            data={
                "id": [0, 0, 0, 0, 1, 2, 3, 4, 5],
                "str_col": [
                    "asddd",
                    "foo",
                    "bar",
                    "baz",
                    "foo",
                    "bar",
                    "baz",
                    "tmp",
                    "mic check 123",
                ],
                "float_col": [0.1, 11.2, 1.1, 2.2, 1.2, 3.4, 99.99, 123.321, -12.32],
            },
            schema={
                "id": pl.Int64,
                "str_col": pl.String,
                "float_col": pl.Float64,
            },
        )
        df = client.read_table(
            catalog=default_catalog,
            schema=default_schema,
            name=table_name,
        )
        assert_frame_equal(df, expected, check_row_order=False)

        client.write_table(
            df=df3,
            catalog=default_catalog,
            schema=default_schema,
            name=table_name,
            mode="overwrite",
            replace_where="id > 4 AND length(str_col) > 3",
        )
        expected = pl.DataFrame(
            data={
                "id": [0, 0, 0, 0, 1, 2, 3, 4, 6, 7],
                "str_col": [
                    "asddd",
                    "foo",
                    "bar",
                    "baz",
                    "foo",
                    "bar",
                    "baz",
                    "tmp",
                    "hiiiii",
                    "mommmm",
                ],
                "float_col": [0.1, 11.2, 1.1, 2.2, 1.2, 3.4, 99.99, 123.321, 0.1, 11.2],
            },
            schema={
                "id": pl.Int64,
                "str_col": pl.String,
                "float_col": pl.Float64,
            },
        )
        df = client.read_table(
            catalog=default_catalog,
            schema=default_schema,
            name=table_name,
        )
        assert_frame_equal(df, expected, check_row_order=False)


def test_partition_filters(
    client: UCClient,
):
    assert client.health_check()

    default_catalog = "unity"
    default_schema = "default"
    table_name = "test_table"

    df1 = pl.DataFrame(
        data={
            "id": [0, 0, 0, 1, 1, 2],
            "str_col": ["asd", "asd", "foo", "asd", "foo", "baz"],
            "float_col": [0.1, 1.2, 3.4, 99.99, 123.321, -12.32],
        },
        schema={
            "id": pl.Int64,
            "str_col": pl.String,
            "float_col": pl.Float64,
        },
    )
    df2 = pl.DataFrame(
        data={
            "id": [0, 0, 0, 0],
            "str_col": ["asddd", "foo", "bar", "baz"],
            "float_col": [0.1, 11.2, 1.1, 2.2],
        },
        schema={
            "id": pl.Int64,
            "str_col": pl.String,
            "float_col": pl.Float64,
        },
    )
    df3 = pl.DataFrame(
        data={
            "id": [6, 7],
            "str_col": ["hiiiii", "mommmm"],
            "float_col": [0.1, 11.2],
        },
        schema={
            "id": pl.Int64,
            "str_col": pl.String,
            "float_col": pl.Float64,
        },
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        client.create_as_table(
            df=df1,
            catalog=default_catalog,
            schema=default_schema,
            name=table_name,
            file_type="delta",
            table_type="external",
            location="file://" + tmpdir,
            partition_cols=["id", "str_col"],
        )

        df = client.read_table(
            catalog=default_catalog,
            schema=default_schema,
            name=table_name,
        )
        assert_frame_equal(df, df1, check_row_order=False)

        client.write_table(
            df=df2,
            catalog=default_catalog,
            schema=default_schema,
            name=table_name,
            mode="overwrite",
            partition_filters=[("id", "=", "0")],
        )
        expected = pl.DataFrame(
            data={
                "id": [0, 0, 0, 0, 1, 1, 2],
                "str_col": ["asddd", "foo", "bar", "baz", "asd", "foo", "baz"],
                "float_col": [0.1, 11.2, 1.1, 2.2, 99.99, 123.321, -12.32],
            },
            schema={
                "id": pl.Int64,
                "str_col": pl.String,
                "float_col": pl.Float64,
            },
        )
        df = client.read_table(
            catalog=default_catalog,
            schema=default_schema,
            name=table_name,
        )
        assert_frame_equal(df, expected, check_row_order=False)

        client.write_table(
            df=df3,
            catalog=default_catalog,
            schema=default_schema,
            name=table_name,
            mode="overwrite",
            partition_filters=[("id", ">", "0"), ("str_col", "!=", "asd")],
        )
        expected = pl.DataFrame(
            data={
                "id": [0, 0, 0, 0, 1, 6, 7],
                "str_col": ["asddd", "foo", "bar", "baz", "asd", "hiiiii", "mommmm"],
                "float_col": [0.1, 11.2, 1.1, 2.2, 99.99, 0.1, 11.2],
            },
            schema={
                "id": pl.Int64,
                "str_col": pl.String,
                "float_col": pl.Float64,
            },
        )
        df = client.read_table(
            catalog=default_catalog,
            schema=default_schema,
            name=table_name,
        )
        assert_frame_equal(df, expected, check_row_order=False)
