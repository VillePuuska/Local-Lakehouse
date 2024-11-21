import requests
import polars as pl
import duckdb
from typing import Literal, Any
from deltalake import DeltaTable
from deltalake.table import TableMerger
from .exceptions import UnsupportedOperationError, DuckDBConnectionSetupError
from .models import Catalog, Schema, Table, TableType, FileType
from .dataframe import (
    WriteMode,
    SchemaEvolution,
    read_table,
    scan_table,
    write_table,
    df_schema_to_uc_schema,
    merge_table,
)
from .uc_api_wrapper import (
    create_catalog,
    create_schema,
    create_table,
    delete_catalog,
    delete_schema,
    delete_table,
    get_catalog,
    get_schema,
    get_table,
    health_check,
    list_catalogs,
    list_schemas,
    list_tables,
    update_catalog,
    update_schema,
    overwrite_table,
    update_table,
    set_table_default_merge_columns,
    sync_delta_properties,
    get_delta_table,
)
from .utils import (
    literal_to_filetype,
    literal_to_schemaevolution,
    literal_to_tabletype,
    literal_to_writemode,
)
import logging

logger = logging.getLogger(__name__)


class UCClient:
    """
    A UCCLient object
        - holds the connections to the Unity Catalog (requests Session, DuckDB connection),
        - exposes methods for interacting with the Unity Catalog.
    """

    def __init__(self, uc_url: str = "http://localhost:8080") -> None:
        self.uc_url = uc_url.removesuffix("/")
        self.session = requests.Session()

        self.conn: duckdb.DuckDBPyConnection | None
        try:
            self.conn = duckdb.connect()
            duckdb_uc_setup = f"""
    install uc_catalog from core_nightly;
    load uc_catalog;
    install delta;
    load delta;
    CREATE SECRET (
        TYPE UC,
        TOKEN 'not-used',
        ENDPOINT '{uc_url}',
        AWS_REGION 'us-east-2'
    );
    ATTACH 'unity' AS unity (TYPE UC_CATALOG);
    """
            self.conn.sql(duckdb_uc_setup)
        except:
            logger.warning("Failed to create a DuckDB connection to Unity Catalog.")
            self.conn = None

    def health_check(self) -> bool:
        """
        Checks that Unity Catalog is running at the specified address.
        """
        return health_check(session=self.session, uc_url=self.uc_url)

    def create_catalog(self, catalog: Catalog) -> Catalog:
        """
        Creates a new catalog with the following fields specified in the parameter `catalog`:
            - name,
            - comment,
            - properties.
        Returns a new Catalog with the following fields added:
            - created_at,
            - id.
        Raises an AlreadyExistsError if a catalog with the name already exists.
        """
        return create_catalog(session=self.session, uc_url=self.uc_url, catalog=catalog)

    def delete_catalog(self, name: str, force: bool = False) -> bool:
        """
        Deletes the catalog with the specified name.`

        If `force == False`, then only deletes if the catalog is empty;
        if `force == True`, deletes the catalog even if it has schemas.

        Returns True/False indicating if a catalog was deleted.
        """
        return delete_catalog(
            session=self.session, uc_url=self.uc_url, name=name, force=force
        )

    def list_catalogs(self) -> list[Catalog]:
        """
        Returns a list of catalogs from the specified Unity Catalog.
        """
        return list_catalogs(session=self.session, uc_url=self.uc_url)

    def get_catalog(self, name: str) -> Catalog:
        """
        Returns the info of the catalog with the specified name, if it exists.
        Raises a DoesNotExistError if a catalog with the name does not exist.
        """
        return get_catalog(session=self.session, uc_url=self.uc_url, name=name)

    def update_catalog(self, name: str, catalog: Catalog) -> Catalog:
        """
        Updates the catalog with the given name with the following fields from `catalog`:
            - name,
            - comment,
            - properties.
        Returns a Catalog with updated information.
        Raises a DoesNotExistError if a catalog with the name does not exist.
        Raises an AlreadyExistsError if the new name is the same as the old name. Unity Catalog
        does not allow updating and keeping the same name atm.
        """
        return update_catalog(
            session=self.session, uc_url=self.uc_url, name=name, catalog=catalog
        )

    def create_schema(self, schema: Schema) -> Schema:
        """
        Creates a new schema with the following fields specified in the parameter `schema`:
            - name,
            - catalog_name,
            - comment,
            - properties.
        Returns a new Schema with the remaining fields populated.
        Raises an AlreadyExistsError if a schema with the name already exists in the same catalog.
        """
        return create_schema(session=self.session, uc_url=self.uc_url, schema=schema)

    def delete_schema(self, catalog: str, schema: str, force: bool = False) -> bool:
        """
        Deletes the schema in the catalog.

        If `force == False`, then only deletes if the schema is empty;
        if `force == True`, deletes the schema even if it has tables.

        Returns True/False indicating if a schema was deleted.
        Raises a DoesNotExistError if a schema with the name does not exist.
        """
        return delete_schema(
            session=self.session,
            uc_url=self.uc_url,
            catalog=catalog,
            schema=schema,
            force=force,
        )

    def get_schema(self, catalog: str, schema: str) -> Schema:
        """
        Returns the info of the schema in the catalog, if it exists.
        Raises a DoesNotExistError if the schema or catalog does not exist.
        """
        return get_schema(
            session=self.session, uc_url=self.uc_url, catalog=catalog, schema=schema
        )

    def list_schemas(self, catalog: str) -> list[Schema]:
        """
        Returns a list of schemas in the specified catalog from Unity Catalog.
        """
        return list_schemas(session=self.session, uc_url=self.uc_url, catalog=catalog)

    def update_schema(
        self, catalog: str, schema_name: str, new_schema: Schema
    ) -> Schema:
        """
        Updates the schema with the given name in the given catalog with the following
        fields from `new_schema`:
            - name,
            - comment,
            - properties.
        Returns a Schema with updated information.
        Raises a DoesNotExistError if the schema does not exist.
        Raises an AlreadyExistsError if there already exists a schema with the new name
        in the same catalog.
        """
        return update_schema(
            uc_url=self.uc_url,
            session=self.session,
            catalog=catalog,
            schema_name=schema_name,
            new_schema=new_schema,
        )

    def create_table(self, table: Table) -> Table:
        """
        Creates a new table with the following fields specified in the parameter `table`:
            - name,
            - catalog_name,
            - schema_name,
            - table_type,
            - file_type,
            - columns,
            - storage_location (for EXTERNAL tables),
            - comment,
            - properties.
        Returns a new Table with the remaining fields populated.
        Raises an AlreadyExistsError if a Table with the name already exists in the same catalog.
        """
        return create_table(session=self.session, uc_url=self.uc_url, table=table)

    def delete_table(
        self,
        catalog: str,
        schema: str,
        table: str,
    ):
        """
        Deletes the table.
        Raises a DoesNotExistError if the table did not exist.
        """
        return delete_table(
            session=self.session,
            uc_url=self.uc_url,
            catalog=catalog,
            schema=schema,
            table=table,
        )

    def get_table(
        self,
        catalog: str,
        schema: str,
        table: str,
    ) -> Table:
        """
        Returns the info of the table, if it exists.
        Raises a DoesNotExistException if the table does not exist.
        """
        return get_table(
            session=self.session,
            uc_url=self.uc_url,
            catalog=catalog,
            schema=schema,
            table=table,
        )

    def list_tables(self, catalog: str, schema: str) -> list[Table]:
        """
        Returns a list of tables in the specified catalog.schema from Unity Catalog.
        """
        return list_tables(
            session=self.session, uc_url=self.uc_url, catalog=catalog, schema=schema
        )

    def update_table(self, catalog: str, schema: str, table: Table) -> Table:
        """
        Updates the table with the given name in the given catalog and schema
        with the following fields from `table`:
            - comment,
            - properties.
        Returns a Table with updated information.
        Raises a DoesNotExistError if the table does not exist.
        """
        return update_table(
            session=self.session,
            uc_url=self.uc_url,
            catalog=catalog,
            schema=schema,
            table=table,
        )

    def set_table_default_merge_columns(
        self,
        catalog: str,
        schema: str,
        table: str,
        merge_columns: list[str],
    ) -> Table:
        """
        Updates the default merge columns for the table.
        Returns a Table with updated information.
        Raises a DoesNotExistError if the table does not exist.
        """
        return set_table_default_merge_columns(
            session=self.session,
            uc_url=self.uc_url,
            catalog=catalog,
            schema=schema,
            table=table,
            merge_columns=merge_columns,
        )

    def read_table(self, catalog: str, schema: str, name: str) -> pl.DataFrame:
        """
        Reads the specified table from Unity Catalog and returns it as a Polars DataFrame.
        """
        table = self.get_table(catalog=catalog, schema=schema, table=name)
        return read_table(table=table)

    def scan_table(self, catalog: str, schema: str, name: str) -> pl.LazyFrame:
        """
        Lazily reads/scans the specified table from Unity Catalog and returns it as a Polars LazyFrame.
        """
        table = self.get_table(catalog=catalog, schema=schema, table=name)
        return scan_table(table=table)

    def get_delta_table(self, catalog: str, schema: str, name: str) -> DeltaTable:
        """
        Returns the specified table from Unity Catalog as a `DeltaTable`.

        Raises an `UnsupportedOperationError` if the table is not DELTA.
        """
        table = self.get_table(catalog=catalog, schema=schema, table=name)
        return get_delta_table(table=table)

    def sync_delta_properties(self, catalog: str, schema: str, name: str) -> Table:
        """
        Syncs the properties of the underlying Delta table with Unity Catalog.
        These are the properties starting with 'delta.'

        Note: this sync only goes one way: Delta table -> Unity Catalog.
        If you need to add delta properties to the underlying Delta table,
        get the table as a `DeltaTable` with `get_delta_table`, use the
        `DeltaTable` to make your changes, and finally sync with Unity Catalog
        with this method.
        """
        return sync_delta_properties(
            session=self.session,
            uc_url=self.uc_url,
            catalog=catalog,
            schema=schema,
            name=name,
        )

    def write_table(
        self,
        df: pl.DataFrame,
        catalog: str,
        schema: str,
        name: str,
        mode: Literal["append", "overwrite"] | WriteMode = WriteMode.APPEND,
        schema_evolution: (
            Literal["strict", "merge", "overwrite"] | SchemaEvolution
        ) = SchemaEvolution.STRICT,
        partition_filters: list[tuple[str, str, Any]] | None = None,
        replace_where: str | None = None,
    ) -> None:
        """
        Writes the Polars DataFrame `df` to the Unity Catalog table
        `catalog.schema.name`. If the table does not already exist, it is created.

        `mode` specifies the writing mode:
            - WriteMode.APPEND to append to the existing table, IF it exists.
            - WriteMode.OVERWRITE replaces/overwrites the existing table, IF it exists.
                - For single file Parquet, CSV, AVRO: overwrites the file.
                - For partitioned Parquet: overwrites the common partitions; DOES NOT ALWAYS OVERWRITE EVERYTHING.
                - For Delta, overwrites everything.

        `schema_evolution` specifies how to handle possible schema mismatches:
            - SchemaEvolution.STRICT raises an Exception if there is a difference in schemas.
            - SchemaEvolution.MERGE will attempt to merge the schemas; raises if impossible.
            - SchemaEvolution.OVERWRITE will attempt to cast the existing table to the schema of the new
              DataFrame; raises if impossible.
        """
        if not isinstance(schema_evolution, SchemaEvolution):
            schema_evolution = literal_to_schemaevolution(schema_evolution)
        if not isinstance(mode, WriteMode):
            mode = literal_to_writemode(mode)
        table = self.get_table(catalog=catalog, schema=schema, table=name)
        new_columns = write_table(
            table=table,
            df=df,
            mode=mode,
            schema_evolution=schema_evolution,
            partition_filters=partition_filters,
            replace_where=replace_where,
        )
        if new_columns is not None:
            try:
                table.columns = new_columns
                overwrite_table(session=self.session, uc_url=self.uc_url, table=table)
            except Exception as e:
                raise Exception("Something went horribly wrong.") from e

    def merge_table(
        self,
        df: pl.DataFrame,
        catalog: str,
        schema: str,
        name: str,
        merge_condition: str | None = None,
        source_alias: str = "s",
        target_alias: str = "t",
    ) -> TableMerger:
        """
        Creates a `TableMerger` to merge the Polars DataFrame `df` to the Unity Catalog table
        `catalog.schema.name` with the condition `merge_condition`.

        If `merge_condition` is not specified, then the default merge columns stored in Unity
        Catalog are used.

        This method only supports exactly matching schemas.
        """
        table = self.get_table(catalog=catalog, schema=schema, table=name)
        return merge_table(
            table=table,
            df=df,
            merge_condition=merge_condition,
            source_alias=source_alias,
            target_alias=target_alias,
        )

    def create_as_table(
        self,
        df: pl.DataFrame,
        catalog: str,
        schema: str,
        name: str,
        file_type: (
            Literal["delta", "csv", "json", "avro", "parquet", "orc", "text"] | FileType
        ) = FileType.DELTA,
        table_type: Literal["managed", "external"] | TableType = TableType.EXTERNAL,
        location: str | None = None,
        partition_cols: list[str] | None = None,
    ) -> Table:
        """
        Creates a new table to Unity Catalog with the schema of the Polars DataFrame `df`
        and writes `df` to the new table. Raises an AlreadyExistsError if the table alredy exists.
        """
        if not isinstance(file_type, FileType):
            file_type = literal_to_filetype(file_type)
        if not isinstance(table_type, TableType):
            table_type = literal_to_tabletype(table_type)
        if table_type == TableType.MANAGED:
            raise UnsupportedOperationError("MANAGED tables are not yet supported.")
        if table_type == TableType.EXTERNAL and location is None:
            raise UnsupportedOperationError(
                "To create an EXTERNAL table, you must specify a location to store it in."
            )
        assert (
            location is not None
        )  # Logically impossible, but Pyright can't figure this out; mypy actually can
        if not location.startswith("file://"):
            raise UnsupportedOperationError(
                "Only local storage is supported. Hint: location must be of the form file://<absolute_path>, e.g. file:///home/me/ex-delta-table"
            )
        cols = df_schema_to_uc_schema(df=df)
        if partition_cols is not None:
            if file_type not in (FileType.DELTA, FileType.PARQUET):
                raise UnsupportedOperationError(
                    "Partitioned tables only supported for DELTA and PARQUET."
                )
            for i, col in enumerate(cols):
                if col.name not in partition_cols:
                    continue
                partition_ind = partition_cols.index(col.name)
                cols[i].partition_index = partition_ind
        table = Table(
            name=name,
            catalog_name=catalog,
            schema_name=schema,
            table_type=table_type,
            file_type=file_type,
            columns=cols,
            storage_location=location,
        )
        table = self.create_table(table=table)
        self.write_table(
            df=df, catalog=catalog, schema=schema, name=name, mode=WriteMode.OVERWRITE
        )
        return table

    def register_as_table(
        self,
        filepath: str,
        catalog: str,
        schema: str,
        name: str,
        file_type: (
            Literal["delta", "csv", "json", "avro", "parquet", "orc", "text"] | FileType
        ),
        partition_cols: list[str] | None = None,
    ) -> Table:
        """
        Creates a new table to Unity Catalog from a table/file at `filepath`.
        Raises an AlreadyExistsError if the table alredy exists.
        """
        if not isinstance(file_type, FileType):
            file_type = literal_to_filetype(file_type)
        if not filepath.startswith("file://"):
            if not filepath.startswith("/"):
                raise UnsupportedOperationError(
                    "Filepath must be absolute, e.g. file:///home/me/ex-delta-table or simply /home/me/ex-delta-table"
                )
            filepath = "file://" + filepath
        table = Table(
            name=name,
            catalog_name=catalog,
            schema_name=schema,
            table_type=TableType.EXTERNAL,
            file_type=file_type,
            columns=[],
            storage_location=filepath,
        )
        df = read_table(table=table)
        cols = df_schema_to_uc_schema(df=df)
        if partition_cols is not None:
            if file_type not in (FileType.DELTA, FileType.PARQUET):
                raise UnsupportedOperationError(
                    "Partitioned tables only supported for DELTA and PARQUET."
                )
            for i, col in enumerate(cols):
                if col.name not in partition_cols:
                    continue
                partition_ind = partition_cols.index(col.name)
                cols[i].partition_index = partition_ind
        table.columns = cols
        table = self.create_table(table=table)
        return table

    def sql(self, query: str) -> duckdb.DuckDBPyRelation:
        """
        Passes the `query` to the DuckDB connection.
        NOTE: at the moment the connection is read only and only supports Delta Tables.
        """
        if self.conn is None:
            raise DuckDBConnectionSetupError
        return self.conn.sql(query=query)
