from typing import Any, Dict, Optional

from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.models.xcom_arg import XComArg

from astro.constants import DEFAULT_CHUNK_SIZE, LoadExistStrategy
from astro.databases import BaseDatabase, create_database
from astro.files import get_files

# from astro.sql.table import Table, TempTable
from astro.sql.tables import Table
from astro.utils.load import populate_normalize_config
from astro.utils.task_id_helper import get_task_id


class AgnosticLoadFile(BaseOperator):
    """Load S3/local table to postgres/snowflake database

    :param path: File path
    :param output_table_name: Name of table to create
    :param file_conn_id: Airflow connection id of input file (optional)
    :param output_conn_id: Database connection id
    :param ndjson_normalize_sep: separator used to normalize nested ndjson.
    """

    template_fields = (
        "output_table",
        "file_conn_id",
        "path",
    )

    def __init__(
        self,
        path: str,
        output_table: Table,
        file_conn_id: Optional[str] = "",
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        if_exists: LoadExistStrategy = "replace",
        ndjson_normalize_sep: str = "_",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.output_table: Table = output_table
        self.path = path
        self.chunk_size = chunk_size
        self.file_conn_id = file_conn_id
        self.kwargs = kwargs
        self.if_exists = if_exists
        self.ndjson_normalize_sep = ndjson_normalize_sep
        self.normalize_config: Dict[str, str] = {}

    def execute(self, context: Any) -> Table:
        """
        Load an existing dataset from a supported file into a SQL table.
        """
        if self.file_conn_id:
            BaseHook.get_connection(self.file_conn_id)

        database = create_database(self.output_table.conn_id)

        self.normalize_config = populate_normalize_config(
            ndjson_normalize_sep=self.ndjson_normalize_sep,
            database=database,
        )

        # self._configure_output_table(context)
        return self.load_data(
            database=database, path=self.path, file_conn_id=self.file_conn_id
        )

    def load_data(
        self, path: str, database: BaseDatabase, file_conn_id: Optional[str] = None
    ) -> Table:
        """Loads csv/parquet table from local/S3/GCS with Pandas.
        Infers SQL database type based on connection then loads table to db.
        """
        self.log.info(f"Loading {self.path} into {self.output_table}...")
        if_exists = self.if_exists
        for file in get_files(
            path, file_conn_id, normalize_config=self.normalize_config
        ):
            database.load_pandas_dataframe_to_table(
                source_dataframe=file.export_to_dataframe(),
                target_table=self.output_table,
                if_exists=if_exists,
                chunk_size=self.chunk_size,
            )
            if_exists = "append"

        self.log.info(f"Completed loading the data into {self.output_table}.")

        return self.output_table

    # def _configure_output_table(self, context: Any) -> None:
    #     # TODO: Move this function to the SQLDecorator, so it can be reused across operators
    #     if isinstance(self.output_table, TempTable):
    #         self.output_table = self.output_table.to_table(
    #             create_table_name(context=context)
    #         )
    #     if not self.output_table.table_name:
    #         self.output_table.table_name = create_table_name(context=context)


def load_file(
    path: str,
    output_table: Table,
    file_conn_id: Optional[str] = "",
    task_id: Optional[str] = None,
    if_exists: LoadExistStrategy = "replace",
    ndjson_normalize_sep: str = "_",
    **kwargs,
) -> XComArg:
    """Convert AgnosticLoadFile into a function that Returns an XComArg object
    :param path: File path
    :param output_table: Table to create
    :param file_conn_id: Airflow connection id of input file (optional)
    :param task_id: task id, optional
    :param if_exists: default override an existing Table. Options: fail, replace, append
    :param ndjson_normalize_sep: separator used to normalize nested ndjson.
        ex - {"a": {"b":"c"}} will result in
            column - "a_b"
            where ndjson_normalize_sep = "_"
    """

    # Note - using path for task id is causing issues as it's a pattern and
    # contain chars like - ?, * etc. Which are not acceptable as task id.
    task_id = task_id if task_id is not None else get_task_id("load_file", "")

    return AgnosticLoadFile(
        task_id=task_id,
        path=path,
        output_table=output_table,
        file_conn_id=file_conn_id,
        if_exists=if_exists,
        ndjson_normalize_sep=ndjson_normalize_sep,
        **kwargs,
    ).output
