# Local Lakehouse: <br> Unity Catalog & Polars & DuckDB to manage and use tables stored in your local filesystem

![CI](https://github.com/VillePuuska/Local-Lakehouse/actions/workflows/CI.yaml/badge.svg)

Toying with the idea of using Unity Catalog to manage my local structured data and accessing it via a (Polars) DataFrame API and (DuckDB) SQL API.

Very much work in progress, but kind of works now. Feel free to request functionality that is still WIP by opening an issue or just send a PR.

*Likely to be abandoned once Unity Catalog gets an official Python SDK.*

## Idea/outline
- Tables are stored as files in the local filesystem.
- Managed tables get a location assigned at creation, for external tables you specify the storage location at creation.
- Supported storage formats: (default) Delta Lake, Parquet (single file & hive partitioned), CSV, AVRO.
- Three layer namespace used in Unity Catalog: `<catalog>.<schema>.<table>`
- You access all (public) functionality through a `UCClient` object.
    - CRUD methods for Unity Catalog's catalogs, schemas, and tables endpoints.
    - `read_table`, `scan_table`, `create_as_table`, and `write_table` for accessing tables in Unity Catalog directly as/with Polars DataFrames/LazyFrames.
- Polars based DataFrame methods:
    - `read_table` and `scan_table` methods read a table from Unity Catalog and return it as a Polars DataFrames/LazyFrames.
    - `create_as_table` and `write_table` methods take in a Polars DataFrame and write it to a table stored in Unity Catalog. Writing LazyFrames not supported, at least for now.
- SQL (**Read-only**) (WIP):
    - A `UCClient` object stores a DuckDB connection o the Unity Catalog that .
    - If you need to save/write to Unity Catalog tables, convert your DuckDB object to a Polars Dataframe and use the DataFrame methods to write.

---

## Notes
- You need to have the Unity Catalog server running locally. See the [Unity Catalog repo](https://github.com/unitycatalog/unitycatalog) for instructions. **No support planned for Databricks Unity Catalog or any kind of authentication with a locally running Unity Catalog.**
    - You can also build and run Unity Catalog in a Docker container using the Dockerfile in the `tests` directory while waiting for an official Unity Catalog image.
- Only tables stored in your local filesystem are supported. **No support planned for storing tables e.g. in s3.**

---

## Tests
- To be able to run integration tests you need
    - a Python environment with the dev dependencies installed (`poetry install --with dev`),
    - Docker or a container engine that works with testcontainers.
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
