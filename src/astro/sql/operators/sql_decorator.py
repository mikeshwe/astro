import inspect
from typing import Callable, Dict, Iterable, Mapping, Optional, Union

import pandas as pd
from airflow.decorators.base import (
    BaseOperator,
    DecoratedOperator,
    task_decorator_factory,
)
from airflow.hooks.base import BaseHook
from airflow.utils.db import provide_session
from sqlalchemy.sql.functions import Function

from astro.constants import Database
from astro.settings import SCHEMA
from astro.sql.table import Table, TempTable, create_unique_table_name
from astro.utils import get_hook, postgres_transform, snowflake_transform
from astro.utils.database import (
    create_database_from_conn_id,
    get_sqlalchemy_engine,
    run_sql,
)
from astro.utils.load import load_dataframe_into_sql_table
from astro.utils.schema_util import create_schema_query, schema_exists
from astro.utils.table_handler import TableHandler


class SqlExecutor(BaseOperator):
    def __init__(
        self,
        sql: Optional[str] = None,
        conn_id: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        role: Optional[str] = None,
        warehouse: Optional[str] = None,
        parameters: Optional[dict] = None,
        handler: Optional[Callable] = None,
        **kwargs,
    ):
        self.conn_id = conn_id
        self.database = database
        self.schema = schema
        self.role = role
        self.warehouse = warehouse
        self.parameters = parameters
        self.sql = sql
        self.handler = handler
        self.kwargs = kwargs or {}

        if self.parameters is None:
            self.parameters = {}

        self.op_kwargs: Dict = self.kwargs.get("op_kwargs") or {}

        super().__init__(
            **kwargs,
        )

    @property
    def hook(self):
        return get_hook(
            conn_id=self.conn_id,
            database=self.database,
            role=self.role,
            schema=self.schema,
            warehouse=self.warehouse,
        )

    @property
    def conn_type(self):
        return BaseHook.get_connection(self.conn_id).conn_type

    def execute(self, context: Dict):
        # handle templating
        self.handle_params(context)

        # update table name in context based on database
        context = self._add_templates_to_context(context)

        # render templating in sql query
        if context:
            self.sql = self.render_template(self.sql, context)

        # update parameters for snowflake table with fully qualified names
        self._process_params()

        cursor = self._run_sql(self.sql, self.parameters)
        if self.handler is not None:
            return self.handler(cursor)
        return cursor

    def _run_sql(self, sql, parameters=None):
        return run_sql(
            engine=self.get_sql_alchemy_engine(),
            sql_statement=sql,
            parameters=parameters,
        )

    def get_sql_alchemy_engine(self):
        return get_sqlalchemy_engine(self.hook)

    # name can be improved.
    # 1. method is merging op_kwargs and op_args into self.parameters
    # 2. then self.parameters rendering templates in self.parameters
    def handle_params(self, context):
        if self.op_kwargs:
            self.parameters.update(self.op_kwargs)  # type: ignore
        # if self.op_args:
        #     params = list(inspect.signature(self.python_callable).parameters.keys())
        #     for i, arg in enumerate(self.op_args):
        #         self.parameters[params[i]] = arg  # type: ignore
        if context:
            self.parameters = {
                k: self.render_template(v, context) for k, v in self.parameters.items()  # type: ignore
            }
        # why are we overriding again??
        self.parameters.update(self.op_kwargs)  # type: ignore

    def _add_templates_to_context(self, context):
        # candidate for database refactor
        database = get_database_from_conn_id(self.conn_id)
        if database in (Database.POSTGRES, Database.BIGQUERY):
            return postgres_transform.add_templates_to_context(self.parameters, context)
        elif database == Database.SNOWFLAKE:
            return snowflake_transform.add_templates_to_context(
                self.parameters, context
            )
        else:
            return self.default_transform(self.parameters, context)

    def default_transform(self, parameters, context):
        for k, v in parameters.items():
            if isinstance(v, Table):
                context[k] = v.table_name
            else:
                # for sqla templating
                context[k] = ":" + k
        return context

    # name can be improved.
    def _process_params(self):
        if self.conn_type == "snowflake":
            self.parameters = snowflake_transform.process_params(
                parameters=self.parameters
            )


class SqlDecoratedOperator(DecoratedOperator, TableHandler):
    def __init__(
        self,
        conn_id: Optional[str] = None,
        autocommit: bool = False,
        parameters: Optional[dict] = None,
        handler: Optional[Function] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        warehouse: Optional[str] = None,
        role: Optional[str] = None,
        raw_sql=False,
        sql="",
        **kwargs,
    ):
        # Init vars
        self.raw_sql = raw_sql
        self.autocommit = autocommit
        self.parameters = parameters
        self.handler = handler
        self.kwargs = kwargs or {}
        self.sql = sql

        # optional args from op_kwargs
        self.op_kwargs: Dict = self.kwargs.get("op_kwargs") or {}

        # get output table from op_kwargs
        if self.op_kwargs.get("output_table"):
            self.output_table: Optional[Table] = self.op_kwargs.pop("output_table")
        else:
            self.output_table = None

        # handler function for processing resultProxy
        if self.op_kwargs.get("handler"):
            self.handler = self.op_kwargs.pop("handler")

        # init db related params via op_kwargs
        self.conn_id = self.op_kwargs.pop("conn_id", conn_id)
        self.database = self.op_kwargs.pop("database", database)
        self.schema = self.op_kwargs.pop("schema", schema)
        self.warehouse = self.op_kwargs.pop("warehouse", warehouse)
        self.role = self.op_kwargs.pop("role", role)

        super().__init__(
            **kwargs,
        )

    @property
    def conn_type(self):
        return BaseHook.get_connection(self.conn_id).conn_type

    @property
    def database_from_conn_id(self):
        return BaseHook.get_connection(self.conn_id).extra_dejson.get("database")

    @property
    def hook(self):
        return get_hook(
            conn_id=self.conn_id,
            database=self.database,
            role=self.role,
            schema=self.schema,
            warehouse=self.warehouse,
        )

    def execute(self, context: Dict):

        # check for string/sqla based sql queries
        if not isinstance(self.sql, str):
            cursor = self._run_sql(self.sql, self.parameters)
            if self.handler is not None:
                return self.handler(cursor)
            return cursor

        # init db related vars
        self._set_variables_from_first_table()
        self.database = self.database or self.database_from_conn_id

        # init db var - schema, user
        conn = BaseHook.get_connection(self.conn_id)
        self.schema = self.schema or SCHEMA
        self.user = conn.login

        # place all the dataframe into sql table from op_kwargs/op_args
        self.load_dataframes_from_op_args_to_sql_table()
        self.load_dataframes_from_op_kwargs_to_sql_table()

        # get sql query
        self.get_sql_query_and_parameters()

        # handle templating
        self.handle_params(context)

        # update table name in context based on database
        context = self._add_templates_to_context(context)

        # render templating in sql query
        if context:
            self.sql = self.render_template(self.sql, context)

        # update parameters for snowflake table with fully qualified names
        self._process_params()

        output_table_name: str = ""

        self._set_schema_if_needed()

        if not self.raw_sql:

            # if the output table don't exist or is a tempTable
            if not self.output_table or type(self.output_table) == TempTable:
                output_table_name = create_unique_table_name()
                self._set_schema_if_needed(schema=SCHEMA)
                full_output_table_name = self.handle_output_table_schema(
                    # Since there is no output table defined we have to assume default schema
                    output_table_name,
                    schema=SCHEMA,
                )
            else:
                output_table_name = self.output_table.table_name
                full_output_table_name = self.handle_output_table_schema(
                    output_table_name, schema=self.output_table.schema
                )

            # drop existing table.
            self._run_sql(
                f"DROP TABLE IF EXISTS {full_output_table_name};", self.parameters
            )
            # create a new table.
            # function name should be renamed to create_table_sql_statement.
            self.sql = self.create_temporary_table(self.sql, full_output_table_name)
        else:
            # If there's no SQL to run we simply return
            if self.sql == "" or not self.sql:
                return

        query_result = self._run_sql(self.sql, self.parameters)
        # Run execute function of subclassed Operator.

        if self.output_table:
            if type(self.output_table) == TempTable:
                self.output_table = self.output_table.to_table(
                    table_name=output_table_name, schema=self.output_table.schema
                )

            self.log.info("Returning table %s", self.output_table)
            # misleading method name - populate_output_table_params()
            self.populate_output_table()
            return self.output_table

        elif self.raw_sql:
            # process raw_sql with handler.
            if self.handler is not None:
                return self.handler(query_result)
            return None
        else:
            self.output_table = Table(
                table_name=output_table_name,
            )
            # misleading method name - populate_output_table_params()
            self.populate_output_table()
            self.log.info("Returning table %s", self.output_table)
            return self.output_table

    def get_sql_query_and_parameters(self):
        # option 1 - explicit pass sql queries/file path via self.sql param
        # option 2 - get sql queries via python callable
        if self.sql == "":
            sql_stuff = self.python_callable(*self.op_args, **self.op_kwargs)
            # If we return two things, assume the second thing is the params
            if len(sql_stuff) == 2:
                self.sql, self.parameters = sql_stuff
            else:
                self.sql = sql_stuff
                self.parameters = {}
        elif self.sql.endswith(".sql"):
            with open(self.sql) as file:
                self.sql = file.read().replace("\n", " ")

    # name can be improved.
    # 1. method is merging op_kwargs and op_args into self.parameters
    # 2. then self.parameters rendering templates in self.parameters
    def handle_params(self, context):
        if self.op_kwargs:
            self.parameters.update(self.op_kwargs)  # type: ignore
        if self.op_args:
            params = list(inspect.signature(self.python_callable).parameters.keys())
            for i, arg in enumerate(self.op_args):
                self.parameters[params[i]] = arg  # type: ignore
        if context:
            self.parameters = {
                k: self.render_template(v, context) for k, v in self.parameters.items()  # type: ignore
            }
        # why are we overriding again??
        self.parameters.update(self.op_kwargs)  # type: ignore

    # why can't we handle this via get_qualified_name() function??
    def handle_output_table_schema(
        self, output_table_name: str, schema: Optional[str] = None
    ) -> str:
        """
        In postgres, we set the schema in the query itself instead of as a query parameter.
        This function adds the necessary {schema}.{table} notation.

        :param output_table_name: Output table name
        :param schema: an optional schema if the output_table has a schema set. Defaults to the temp schema
        """
        schema = schema or SCHEMA
        database = create_database_from_conn_id(self.conn_id)
        if database in (Database.POSTGRES, Database.BIGQUERY) and self.schema:
            output_table_name = schema + "." + output_table_name
        elif database == Database.SNOWFLAKE and self.schema and "." not in self.sql:
            output_table_name = self.database + "." + schema + "." + output_table_name
        return output_table_name

    # name can be improved. - create_if_needed()
    def _set_schema_if_needed(self, schema=None):
        # below line should use constants
        if schema is None:
            schema = self.schema

        database_expects_schema = self.conn_type in [
            "postgres",
            "snowflake",
            "bigquery",
        ]
        schema_exist_ = schema_exists(
            hook=self.hook, schema=schema, conn_type=self.conn_type
        )

        if database_expects_schema and not schema_exist_:
            schema_statement = create_schema_query(
                conn_type=self.conn_type,
                hook=self.hook,
                schema_id=schema,
                user=self.user,
            )
            self._run_sql(schema_statement, {})

    def get_sql_alchemy_engine(self):
        return get_sqlalchemy_engine(self.hook)

    def _run_sql(self, sql, parameters=None):
        return run_sql(
            engine=self.get_sql_alchemy_engine(),
            sql_statement=sql,
            parameters=parameters,
        )

    def create_temporary_table(self, query, output_table_name, schema=None):
        """
        Create a temp table for the current task instance. This table will be overwritten if
        the DAG is run again as this table is only ever meant to be temporary.

        :param query: The Query to run
        :param output_table_name: The name of the table to create
        :param schema: The schema to create the table in
        """

        def clean_trailing_semicolon(query):
            query = query.strip()
            if query and query[-1] == ";":
                query = query[:-1]
            return query

        # handle this in get_qualified name of database
        if schema:
            output_table_name = f"{schema}.{output_table_name}"

        if self.conn_type == "sqlite":
            statement = f"CREATE TABLE {output_table_name} AS {clean_trailing_semicolon(query)};"
        else:
            statement = f"CREATE TABLE {output_table_name} AS ({clean_trailing_semicolon(query)});"
        return statement

    @provide_session
    def pre_execute(self, context, session=None):
        """This hook is triggered right before self.execute() is called."""
        pass

    def post_execute(self, context, result=None):
        """
        This hook is triggered right after self.execute() is called.
        """
        pass

    # name can be improved.
    def _process_params(self):
        if self.conn_type == "snowflake":
            self.parameters = snowflake_transform.process_params(
                parameters=self.parameters
            )

    def _add_templates_to_context(self, context):
        # candidate for database refactor
        database = create_database_from_conn_id(self.conn_id)
        if database in (Database.POSTGRES, Database.BIGQUERY):
            return postgres_transform.add_templates_to_context(self.parameters, context)
        elif database == Database.SNOWFLAKE:
            return snowflake_transform.add_templates_to_context(
                self.parameters, context
            )
        else:
            return self.default_transform(self.parameters, context)

    def default_transform(self, parameters, context):
        for k, v in parameters.items():
            if isinstance(v, Table):
                context[k] = v.table_name
            else:
                # for sqla templating
                context[k] = ":" + k
        return context

    def _cleanup(self):
        """Remove DAG's objects from S3 and db."""
        # To-do
        pass

    # misleading name - should be load_dataframes_from_op_args_to_sql_table
    def load_dataframes_from_op_args_to_sql_table(self):
        final_args = []
        for i, arg in enumerate(self.op_args):
            if type(arg) == pd.DataFrame:
                pandas_dataframe = arg
                output_table_name = create_unique_table_name()
                output_table = Table(
                    table_name=output_table_name,
                    conn_id=self.conn_id,
                    database=self.database,
                    schema=self.schema,
                    warehouse=self.warehouse,
                )
                hook = get_hook(
                    conn_id=self.conn_id,
                    database=self.database,
                    schema=self.schema,
                    warehouse=self.warehouse,
                )
                load_dataframe_into_sql_table(pandas_dataframe, output_table, hook)
                final_args.append(output_table)
            else:
                final_args.append(arg)
            self.op_args = tuple(final_args)

    # misleading name - should be load_dataframes_from_op_kwargs_to_sql_table
    def load_dataframes_from_op_kwargs_to_sql_table(self):
        final_kwargs = {}
        for key, value in self.op_kwargs.items():
            if type(value) == pd.DataFrame:
                pandas_dataframe = value
                output_table_name = create_unique_table_name()
                output_table = Table(
                    table_name=output_table_name,
                    conn_id=self.conn_id,
                    database=self.database,
                    schema=self.schema,
                    warehouse=self.warehouse,
                )
                hook = get_hook(
                    conn_id=self.conn_id,
                    database=self.database,
                    schema=self.schema,
                    warehouse=self.warehouse,
                )
                load_dataframe_into_sql_table(pandas_dataframe, output_table, hook)
                final_kwargs[key] = output_table
            else:
                final_kwargs[key] = value
        self.op_kwargs = final_kwargs


def _transform_task(
    python_callable: Optional[Callable] = None,
    multiple_outputs: Optional[bool] = None,
    **kwargs,
):
    """
    Python operator decorator. Wraps a function into an Airflow operator.
    Accepts kwargs for operator kwarg. Can be reused in a single DAG.

    :param python_callable: Function to decorate
    :type python_callable: Optional[Callable]
    :param multiple_outputs: if set, function return value will be
        unrolled to multiple XCom values. List/Tuples will unroll to xcom values
        with index as key. Dict will unroll to xcom values with keys as XCom keys.
        Defaults to False.
    :type multiple_outputs: bool
    """
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=SqlDecoratedOperator,  # type: ignore
        **kwargs,
    )


def transform_decorator(
    python_callable: Optional[Callable] = None,
    multiple_outputs: Optional[bool] = None,
    conn_id: str = "",
    autocommit: bool = False,
    parameters: Optional[Union[Mapping, Iterable]] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    warehouse: Optional[str] = None,
    raw_sql: bool = False,
    handler: Optional[Callable] = None,
):
    """
    :param python_callable:
    :param multiple_outputs:
    :param postgres_conn_id: The :ref:`postgres conn id <howto/connection:postgres>`
        reference to a specific postgres database.
    :type postgres_conn_id: str
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    :type autocommit: bool
    :param parameters: (optional) the parameters to render the SQL query with.
    :type parameters: dict or iterable
    :param database: name of database which overwrite defined one in connection
    :type database: str
    @return:
    """
    return _transform_task(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        conn_id=conn_id,
        autocommit=autocommit,
        parameters=parameters,
        database=database,
        schema=schema,
        warehouse=warehouse,
        raw_sql=raw_sql,
        handler=handler,
    )
