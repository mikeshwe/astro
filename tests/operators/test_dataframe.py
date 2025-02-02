import logging
import pathlib

import pandas
import pytest
from airflow.models.xcom import XCom
from airflow.utils import timezone

import astro.sql as aql
from astro import dataframe as df
from astro.constants import SUPPORTED_DATABASES, Database
from astro.settings import SCHEMA
from astro.sql.table import Table
from tests.operators import utils as test_utils

# Import Operator

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)
CWD = pathlib.Path(__file__).parent


@pytest.mark.parametrize("sql_server", SUPPORTED_DATABASES, indirect=True)
@pytest.mark.parametrize(
    "test_table",
    [
        {
            "path": str(CWD) + "/../data/homes2.csv",
            "load_table": True,
            "is_temp": False,
            "param": {
                "schema": SCHEMA,
                "table_name": test_utils.get_table_name("test_stats_check_2"),
            },
        }
    ],
    indirect=True,
)
def test_dataframe_from_sql_basic(sample_dag, sql_server, test_table):
    """Test basic operation of dataframe operator."""

    @df
    def my_df_func(df: pandas.DataFrame):  # skipcq: PY-D0003
        return df.sell.count()

    with sample_dag:
        f = my_df_func(df=test_table)

    test_utils.run_dag(sample_dag)

    assert (
        XCom.get_one(execution_date=DEFAULT_DATE, key=f.key, task_id=f.operator.task_id)
        == 5
    )


@pytest.mark.parametrize("sql_server", SUPPORTED_DATABASES, indirect=True)
@pytest.mark.parametrize(
    "test_table",
    [
        {
            "path": str(CWD) + "/../data/homes2.csv",
            "load_table": True,
            "is_temp": False,
            "param": {
                "schema": SCHEMA,
                "table_name": test_utils.get_table_name("test_stats_check_2"),
            },
        }
    ],
    indirect=True,
)
def test_dataframe_from_sql_custom_task_id(sample_dag, sql_server, test_table):
    """Test custom and taskId increment when same task is added multiple times."""

    @df(task_id="foo")
    def my_df_func(df: pandas.DataFrame):  # skipcq: PY-D0003
        return df.sell.count()

    with sample_dag:
        for _ in range(5):
            # ensure we can create multiple tasks
            my_df_func(df=test_table)

    task_ids = [x.task_id for x in sample_dag.tasks]
    assert task_ids == ["foo", "foo__1", "foo__2", "foo__3", "foo__4"]


@pytest.mark.parametrize("sql_server", SUPPORTED_DATABASES, indirect=True)
@pytest.mark.parametrize(
    "test_table",
    [
        {
            "path": str(CWD) + "/../data/homes2.csv",
            "load_table": True,
            "is_temp": False,
            "param": {
                "schema": SCHEMA,
                "table_name": test_utils.get_table_name("test_stats_check_2"),
            },
        }
    ],
    indirect=True,
)
def test_dataframe_from_sql_basic_op_arg(sample_dag, sql_server, test_table):
    """Test basic operation of dataframe operator with op_args."""

    @df(conn_id=test_table.conn_id, database=test_table.database)
    def my_df_func(df: pandas.DataFrame):  # skipcq: PY-D0003
        return df.sell.count()

    with sample_dag:
        res = my_df_func(test_table)
    test_utils.run_dag(sample_dag)

    assert (
        XCom.get_one(
            execution_date=DEFAULT_DATE, key=res.key, task_id=res.operator.task_id
        )
        == 5
    )


@pytest.mark.parametrize("sql_server", SUPPORTED_DATABASES, indirect=True)
@pytest.mark.parametrize(
    "test_table",
    [
        {
            "path": str(CWD) + "/../data/homes2.csv",
            "load_table": True,
            "is_temp": False,
            "param": {
                "schema": SCHEMA,
                "table_name": test_utils.get_table_name("test_stats_check_2"),
            },
        }
    ],
    indirect=True,
)
def test_dataframe_from_sql_basic_op_arg_and_kwarg(sample_dag, sql_server, test_table):
    """Test dataframe creation from table object in args and kwargs."""

    @df(conn_id=test_table.conn_id, database=test_table.database)
    def my_df_func(df_1: pandas.DataFrame, df_2: pandas.DataFrame):  # skipcq: PY-D0003
        return df_1.sell.count() + df_2.sell.count()

    with sample_dag:
        res = my_df_func(test_table, df_2=test_table)
    test_utils.run_dag(sample_dag)

    assert (
        XCom.get_one(
            execution_date=DEFAULT_DATE, key=res.key, task_id=res.operator.task_id
        )
        == 10
    )


@pytest.mark.parametrize("sql_server", [Database.SNOWFLAKE.value], indirect=True)
@pytest.mark.parametrize(
    "test_table",
    [
        {
            "path": str(CWD) + "/../data/homes2.csv",
            "load_table": True,
            "is_temp": False,
            "param": {
                "schema": SCHEMA,
                "table_name": test_utils.get_table_name("test_stats_check_2"),
            },
        }
    ],
    indirect=True,
)
@pytest.mark.parametrize(
    "identifiers_as_lower",
    [True, False],
    ids=["identifiers_as_lower=True", "identifiers_as_lower=False"],
)
def test_snow_dataframe_with_lower_and_upper_case(
    sample_dag, sql_server, test_table, identifiers_as_lower
):
    """
    Test dataframe operator 'identifiers_as_lower' param which converts
    all col names in lower case, which is useful to maintain consistency,
    since snowflake return all col name in caps.
    """

    @df(identifiers_as_lower=identifiers_as_lower)
    def my_df_func(df: pandas.DataFrame):  # skipcq: PY-D0003
        return df.columns

    with sample_dag:
        res = my_df_func(df=test_table)
    test_utils.run_dag(sample_dag)

    columns = XCom.get_one(
        execution_date=DEFAULT_DATE, key=res.key, task_id=res.operator.task_id
    )
    assert all(x.islower() for x in columns) == identifiers_as_lower


def test_postgres_dataframe_without_table_arg(sample_dag):
    """Test dataframe operator without table argument"""

    @df
    def validate_result(df: pandas.DataFrame):  # skipcq: PY-D0003
        assert df.iloc[0].to_dict()["colors"] == "red"

    @df
    def sample_df():  # skipcq: PY-D0003
        return pandas.DataFrame(
            {"numbers": [1, 2, 3], "colors": ["red", "white", "blue"]}
        )

    @aql.transform
    def sample_pg(input_table: Table):  # skipcq: PY-D0003
        return "SELECT * FROM {{input_table}}"

    with sample_dag:
        plain_df = sample_df()
        pg_df = sample_pg(
            conn_id="postgres_conn", database="pagila", input_table=plain_df
        )
        validate_result(pg_df)
    test_utils.run_dag(sample_dag)
