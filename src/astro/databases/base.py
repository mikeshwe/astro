from abc import ABC
from typing import Optional

import pandas as pd
import sqlalchemy
from airflow.hooks.base import BaseHook

from astro.constants import (
    DEFAULT_CHUNK_SIZE,
    AppendConflictStrategy,
    ExportExistsStrategy,
    LoadExistStrategy,
)
from astro.exceptions import NonExistentTableException
from astro.files import File
from astro.sql.tables import Table


class BaseDatabase(ABC):
    """
    Base class to represent all the Database interactions.

    The goal is to be able to support new databases by adding
    a new module to the `astro/databases` directory, without the need of
    changing other modules and classes.

    The exception is if the Airflow connection type does not match the new
    Database module name. In that case, we should update the dictionary
    `CUSTOM_CONN_TYPE_TO_MODULE_PATH` available at `astro/databases/__init__.py`.
    """

    _create_schema_statement: str = "CREATE SCHEMA IF NOT EXISTS {}"
    _drop_table_statement: str = "DROP TABLE IF EXISTS {}"
    _create_table_statement: str = "CREATE TABLE IF NOT EXISTS {} AS {}"

    def __init__(self, conn_id: str):
        self.conn_id = conn_id

    @property
    def hook(self) -> BaseHook:
        """Return an instance of the database-specific Airflow hook."""
        raise NotImplementedError

    def replace_illegal_columns_chars(self, char: str) -> str:
        """Return an instance of the database-specific Airflow hook."""
        return char

    @property
    def connection(self) -> sqlalchemy.engine.base.Connection:
        """Return a Sqlalchemy connection object for the given database."""
        return self.sqlalchemy_engine.connect()

    @property
    def sqlalchemy_engine(self) -> sqlalchemy.engine.base.Engine:
        """Return Sqlalchemy engine."""
        return self.hook.get_sqlalchemy_engine()

    def run_sql(self, sql_statement: str, parameters: Optional[dict] = None):
        """
        Return the results to running a SQL statement.

        Whenever possible, this method should be implemented using Airflow Hooks,
        since this will simplify the integration with Async operators.

        :param sql_statement: Contains SQL query to be run against database
        :param parameters: Optional parameters to be used to render the query
        """
        if parameters is None:
            parameters = {}
        return self.connection.execute(sql_statement, parameters)

    def table_exists(self, table: Table) -> bool:
        """
        Check if a table exists in the database.

        :param table: Details of the table we want to check that exists
        """
        table_qualified_name = self.get_table_qualified_name(table)
        inspector = sqlalchemy.inspect(self.sqlalchemy_engine)
        return bool(inspector.dialect.has_table(self.connection, table_qualified_name))

    # ---------------------------------------------------------
    # Table metadata
    # ---------------------------------------------------------
    def get_table_qualified_name(self, table: Table) -> str:  # skipcq: PYL-R0201
        """
        Return table qualified name. This is Database-specific.
        For instance, in Sqlite this is the table name. In Snowflake, however, it is the database, schema and table

        :param table: The table we want to retrieve the qualified name for.
        """
        # Initially this method belonged to the Table class.
        # However, in order to have an agnostic table class implementation,
        # we are keeping all methods which vary depending on the database within the Database class.
        schema = table.metadata.schema if table.metadata else None
        qualified_name: str = f"{schema}.{table.name}" if schema else table.name
        return qualified_name

    # ---------------------------------------------------------
    # Table creation & deletion methods
    # ---------------------------------------------------------
    def create_table(self, table: Table) -> None:
        """
        Create a SQL table. For this method to work, the table instance must contain columns.

        :param table: The table to be created.
        """
        metadata = table.sqlalchemy_metadata
        sqlalchemy_table = sqlalchemy.Table(table.name, metadata, *table.columns)
        metadata.create_all(self.sqlalchemy_engine, tables=[sqlalchemy_table])

    def create_table_from_select_statement(
        self,
        statement: str,
        target_table: Table,
        parameters: Optional[dict] = None,
    ) -> None:
        """
        Export the result rows of a query statement into another table.

        :param statement: SQL query statement
        :param target_table: Destination table where results will be recorded.
        :param parameters: (Optional) parameters to be used to render the SQL query
        """
        statement = self._create_table_statement.format(
            self.get_table_qualified_name(target_table), statement
        )
        self.run_sql(statement, parameters)

    def drop_table(self, table: Table) -> None:
        """
        Delete a SQL table, if it exists.

        :param table: The table to be deleted.
        """
        statement = self._drop_table_statement.format(
            self.get_table_qualified_name(table)
        )
        self.run_sql(statement)

    # ---------------------------------------------------------
    # Table load methods
    # ---------------------------------------------------------
    def load_file_to_table(
        self,
        source_file: File,
        target_table: Table,
        if_exists: LoadExistStrategy = "replace",
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> None:
        """
        Create a table with the file's contents.
        If the table already exists, append or replace the content, depending on the value of `if_exists`.

        :param source_file: Local or remote filepath
        :param target_table: Table in which the file will be loaded
        :param if_exists: Strategy to be used in case the target table already exists.
        :param chunk_size: Specify the number of rows in each batch to be written at a time.
        """
        self.load_pandas_dataframe_to_table(
            source_file.export_to_dataframe(),
            target_table,
            if_exists,
            chunk_size,
        )

    def load_pandas_dataframe_to_table(
        self,
        source_dataframe: pd.DataFrame,
        target_table: Table,
        if_exists: LoadExistStrategy = "replace",
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> None:
        """
        Create a table with the dataframe's contents.
        If the table already exists, append or replace the content, depending on the value of `if_exists`.

        :param source_dataframe: Local or remote filepath
        :param target_table: Table in which the file will be loaded
        :param if_exists: Strategy to be used in case the target table already exists.
        :param chunk_size: Specify the number of rows in each batch to be written at a time.
        """
        source_dataframe.to_sql(
            self.get_table_qualified_name(target_table),
            con=self.sqlalchemy_engine,
            if_exists=if_exists,
            chunksize=chunk_size,
            method="multi",
            index=False,
        )

    def append_table(
        self,
        source_table: Table,
        target_table: Table,
        if_conflicts: AppendConflictStrategy = "exception",
    ):
        """
        Append the source table rows into a destination table.
        The argument `if_conflicts` allows the user to define how to handle conflicts.

        :param source_table: Contains the rows to be appended to the target_table
        :param target_table: Contains the destination table in which the rows will be appended
        :param if_conflicts: The strategy to be applied if there are conflicts.
        """
        raise NotImplementedError

    # ---------------------------------------------------------
    # Extract methods
    # ---------------------------------------------------------
    def export_table_to_pandas_dataframe(self, source_table: Table) -> pd.DataFrame:
        """
        Copy the content of a table to an in-memory Pandas dataframe.

        :param source_table: An existing table in the database
        """
        table_qualified_name = self.get_table_qualified_name(source_table)
        if self.table_exists(source_table):
            return pd.read_sql(
                # We are avoiding SQL injection by confirming the table exists before this statement
                f"SELECT * FROM {table_qualified_name}",  # skipcq BAN-B608
                con=self.sqlalchemy_engine,
            )

        raise NonExistentTableException(
            "The table %s does not exist" % table_qualified_name
        )

    def export_table_to_file(
        self,
        source_table: Table,
        target_file: File,
        if_exists: ExportExistsStrategy = "exception",
    ) -> None:
        """
        Copy the content of a table to a target file of supported type, in a supported location.

        :param source_table: An existing table in the database
        :param target_file: The path to the file to which we aim to dump the content of the database.
        """
        if if_exists == "exception" and target_file.exists():
            raise FileExistsError(f"The file {target_file} already exists.")

        df = self.export_table_to_pandas_dataframe(source_table)
        target_file.create_from_dataframe(df)
