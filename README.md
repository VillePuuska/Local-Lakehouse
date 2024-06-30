# Local Lakehouse: <br> Unity Catalog & Polars & DuckDB to manage and use tables stored in your local filesystem

Toying with the idea of using Unity Catalog to manage my local structured data and accessing it via a (Polars) Dataframe API and (DuckDB) SQL API.

## Initial idea/outline
- Tables are stored as files in the local filesystem.
- Managed tables get a location assigned at creation, for external tables you specify the storage location at creation.
- Supported storage formats: TBD, default Delta Lake.
- Three layer namespace used in Unity Catalog: `<catalog>.<schema>.<table>`
- Python Dataframe API:
    - Polars based: all get/read methods return Polars Dataframes, all create/write methods take in a Polars Dataframe.
    - You can `list`, `create`, and `delete` catalogs, schemas in a catalog, tables in a schema.
    - You can `read` and `scan` tables (returns Polars Dataframes).
    - You can `write` Polars Dataframes to tables.
        - Supported write modes (overwrite, append, merge, etc.) depend on the underlying storage format.
- SQL API:
    - **Read-only**: You can simply use DuckDB to read tables managed in Unity Catalog.
    - **Read-only**: You can use the Python API `sql` method to run DuckDB SQL queries and get the results as a DuckDBPyRelation.
    - If you need to save/write to Unity Catalog tables, use DuckDB Python API, convert what you need to write to a Polars Dataframe, and use the Dataframe API to write.

---

## Notes
- You need to have the Unity Catalog server running locally. See the [Unity Catalog repo](https://github.com/unitycatalog/unitycatalog) for instructions. **No support planned for any kind of authentication.**
    - Alternatively, if you can build and run containers you can use the Dockerfile in the `tests` directory.
- Only tables stored in your local filesystem are supported. **No support planned for storing tables e.g. in s3.**

---

## Tests
- To be able to run integration tests you need
    - a Python environment with the dev dependencies installed (`poetry install --with dev`),
    - Docker.
- The integration tests use testcontainers for spinning up a Unity Catalog server. You have two options for building the needed image:
    - Don't do anything yourself and let the test build the image everytime it runs. Suitable for CI, but annoyingly slow when trying to run tests while developing.
    - Build the image yourself and have the tests use the image you built. To do this:
        - Dockerfile is in the `tests` directory.
        - Set the environment variable `UC_TEST_USE_IMAGE` to `TRUE`; the tests check this to see if a pre-existing image should be used.
        - Set the environment variable `UC_TEST_IMAGE_TAG` to be the tag of the image you built.
        - For example:
        ```
        docker build -t uc-test:0.1 tests/
        export UC_TEST_USE_IMAGE=TRUE
        export UC_TEST_IMAGE_TAG=uc-test:0.1
        ```
- Call `pytest` (or `python -m pytest`) from the root of the repo or from the `tests` directory.
