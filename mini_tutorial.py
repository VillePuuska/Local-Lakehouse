from uchelper import UCClient
import polars as pl
import os

TUTORIAL_DIR = "/home/codespace/tutorial"

# Create a csv-file to start with.
df = pl.DataFrame(
    {
        "id": [0, 1, 2, 3],
        "filter_col": [True, True, False, False],
        "float_col": [1.1, 2.2, 3.3, 4.4],
    }
)
df.write_csv(file=os.path.join(TUTORIAL_DIR, "data.csv"))
print("Original DataFrame:")
print(df)
print("-" * 50)

# All interaction with Unity Catalog is done through a `UCClient` object.
client = UCClient()

# We can register the csv-file we created as the table unity.default.csv_table in Unity Catalog
# with `register_as_table`.
client.register_as_table(
    filepath=os.path.join(TUTORIAL_DIR, "data.csv"),
    catalog="unity",
    schema="default",
    name="csv_table",
    file_type="csv",
)

# Now we can read the created table in as a dataframe.
df_read = client.read_table(catalog="unity", schema="default", name="csv_table")
print("Table read from Unity Catalog:")
print(df_read)
print("-" * 50)

# Let's then filter the dataframe by filter_col and write the resulting dataframe to a
# Delta table and register it to Unity Catalog with `create_as_table`. We'll name it
# unity.default.filtered_delta_table
df_filtered = df_read.filter(pl.col("filter_col") == True)
print("Filtered DataFrame:")
print(df_filtered)
print("-" * 50)
client.create_as_table(
    df=df_filtered,
    catalog="unity",
    schema="default",
    name="filtered_delta_table",
    file_type="delta",
    table_type="external",
    location="file://" + os.path.join(TUTORIAL_DIR, "delta_dir"),
)

# Let's read back the table to make sure we wrote what we wanted to the Delta table.
df_read = client.read_table(
    catalog="unity", schema="default", name="filtered_delta_table"
)
print("Filtered Delta table read from Unity Catalog:")
print(df_read)
print("-" * 50)


# We can write more data to the table with `write_table`. Let's append the entire original
# dataframe `df` to the Delta table we created.
client.write_table(
    df=df,
    catalog="unity",
    schema="default",
    name="filtered_delta_table",
    mode="append",
    schema_evolution="strict",
)

# Now let's just verify we actually wrote more data to the Delta table.
df_read = client.read_table(
    catalog="unity", schema="default", name="filtered_delta_table"
)
print(
    "Delta table read from Unity Catalog after appending the original DataFrame to it:"
)
print(df_read)
print("-" * 50)

# You can also use DuckDB to query (Delta) tables in Unity Catalog.
# The client already has a DuckDB connection to the Unity Catalog set up,
# so you can run your queries with `client.sql()` without needing to set
# anything up yourself.
print("Table queried from Unity Catalog with DuckDB:")
print(client.sql("FROM unity.default.filtered_delta_table WHERE filter_col = FALSE"))
