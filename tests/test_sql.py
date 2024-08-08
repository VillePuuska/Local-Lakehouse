import tempfile
import polars as pl
from polars.testing import assert_frame_equal
import tempfile
from typing import Callable
from uchelper import (
    UCClient,
    Catalog,
    Schema,
)


def test_sql(
    client: UCClient,
    random_df: Callable[[], pl.DataFrame],
):
    assert client.health_check()

    default_catalog = "unity"
    default_schema = "default"
    table_name = "test_table"

    client.create_catalog(
        catalog=Catalog(
            name="test_cat",
        )
    )
    client.create_schema(
        schema=Schema(
            name=default_schema,
            catalog_name="test_cat",
        )
    )

    client.sql("ATTACH 'test_cat' AS test_cat (TYPE UC_CATALOG)")

    for cat_name in [default_catalog, "test_cat"]:
        # DuckDB does not support DECIMAL
        df1 = (
            random_df()
            .with_columns(pl.lit(1).alias("source"))
            .cast({"decimals": pl.Float64})
        )
        df2 = (
            random_df()
            .with_columns(pl.lit(2).alias("source"))
            .cast({"decimals": pl.Float64})
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            client.create_as_table(
                df=pl.concat([df1, df2]),
                catalog=cat_name,
                schema=default_schema,
                name=table_name,
                file_type="delta",
                table_type="external",
                location="file://" + tmpdir,
            )

            df_sql = client.sql(f"FROM {cat_name}.{default_schema}.{table_name}").pl()
            assert_frame_equal(pl.concat([df1, df2]), df_sql, check_row_order=False)

            df_sql2 = client.sql(
                f"FROM {cat_name}.{default_schema}.{table_name} WHERE source = 1"
            ).pl()
            assert_frame_equal(df1, df_sql2, check_row_order=False)
