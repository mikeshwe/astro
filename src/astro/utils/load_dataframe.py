import os
from typing import Optional, Union

from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from pandas import DataFrame
from pandas.io.sql import SQLDatabase
from sqlalchemy import create_engine

from astro.utils import get_hook
from astro.utils.dependencies import (
    BigQueryHook,
    PostgresHook,
    SnowflakeHook,
    pandas_tools,
)
from astro.utils.schema_util import create_schema_query, schema_exists


def move_dataframe_to_sql(
    output_table_name,
    conn_id,
    database,
    schema,
    warehouse,
    conn_type,
    role,
    df: DataFrame,
    user,
    chunksize,
    if_exists="replace",
):
    hook = get_hook(
        conn_id=conn_id, database=database, schema=schema, warehouse=warehouse
    )

    if conn_type != "sqlite" and not schema_exists(
        hook=hook, schema=schema, conn_type=conn_type
    ):
        schema_query = create_schema_query(
            conn_type=conn_type, hook=hook, schema_id=schema, user=user
        )
        hook.run(schema_query)

    if conn_type == "snowflake":

        db = SQLDatabase(engine=hook.get_sqlalchemy_engine())
        # make columns uppercase to prevent weird errors in snowflake
        df.columns = df.columns.str.upper()
        db.prep_table(
            df,
            output_table_name.lower(),
            schema=schema,
            if_exists=if_exists,
            index=False,
        )
        pandas_tools.write_pandas(
            hook.get_conn(),
            df,
            output_table_name,
            chunk_size=chunksize,
            quote_identifiers=False,
        )
    elif conn_type == "bigquery":
        df.to_gbq(
            f"{schema}.{output_table_name}",
            if_exists=if_exists,
            chunksize=chunksize,
            project_id=hook.project_id,
        )
    elif conn_type == "sqlite":
        uri = hook.get_uri().replace("///", "////")
        engine = create_engine(uri)
        df.to_sql(
            output_table_name,
            con=engine,
            if_exists=if_exists,
            chunksize=chunksize,
            method="multi",
            index=False,
        )
    else:
        df.to_sql(
            output_table_name,
            con=hook.get_sqlalchemy_engine(),
            schema=schema,
            if_exists=if_exists,
            chunksize=chunksize,
            method="multi",
            index=False,
        )
