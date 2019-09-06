import pandas as pd
from sqlalchemy import engine

from typing import Callable
import pickle


def create_table(conn: engine.base.Connection, table_name: str) -> None:
    create_table_query = """
        create table {} (
            "column_name" CHARACTER(20) not null,
            "date" DATETIME not null,
            "metric" CHARACTER(30) not null,
            "arguments" CHARACTER(100) null,
            "type" CHARACTER not null DEFAULT 'custom',
            "return_format" CHARACTER DEFAULT 'float',
            "standard_viz" CHARACTER(100) null,
            "is_static" BOOLEAN null DEFAULT true,
            "key_function" BOOLEAN null DEFAULT FALSE,
            "batch_name" CHARACTER null DEFAULT true,
            "valueID" CHARACTER(36) null 
        );
    """.format(
        table_name
    )
    conn.execute(create_table_query)


def create_value_table(conn: engine.base.Connection, table_name: str) -> None:
    create_table_query = """
        create table {} (
            value varchar,
            valueID CHARACTER(36),
                foreign key (valueID) references {}(valueID)
        );
    """.format(
        table_name, table_name.replace("_values", "")
    )
    conn.execute(create_table_query)


def create_custom_value_table(conn: engine.base.Connection, table_name: str) -> None:
    create_table_query = """
        create table {} (
            value BLOB,
            valueID CHARACTER(36),
                foreign key (valueID) references {}(valueID)
        );
    """.format(
        table_name, table_name.replace("_values_custom", "")
    )
    conn.execute(create_table_query)


def create_alert_table(conn: engine.base.Connection, table_name: str) -> None:
    create_table_query = """
        create table {} (
            "column" CHARACTER(20) not null,
            "std_away" NUMERIC not null,
            "value" NUMERIC not null,
            "alert_message" CHARACTER(100) null,
            "date" DATETIME not null
        );
    """.format(
        table_name
    )
    conn.execute(create_table_query)


def get_table(engine: engine.base.Engine, table_name: str) -> pd.DataFrame:
    return pd.read_sql("select * from {}".format(table_name), engine)


def get_all_values(engine: engine.base.Engine, table_name: str) -> pd.DataFrame:
    value_table = table_name + "_values"
    query = f"""
    select *
    from {table_name}
    join (select * from {value_table} UNION select * from {table_name + '_values_custom'}) as {value_table + '_all'}
    on {table_name}.valueID = {value_table + '_all'}.valueID;
    """
    return pd.read_sql(query, engine)


def delete_data(
    conn: engine.base.Connection, name: str, create_function: Callable
) -> None:
    try:
        conn.execute("drop table {}".format(name))
    except:
        pass
    create_table_if_not_exists(conn, name, create_function)


def create_table_if_not_exists(
    conn: engine.base.Connection, name: str, create_function: Callable
) -> None:
    exists = conn.execute(
        "select name from sqlite_master "
        'where type="table" '
        'and name="{}"'.format(name)
    ).fetchone()
    if not exists:
        create_function(conn, name)


def _unpickle(row):
    if row["type"] == "standard_viz_static" or row["type"] == "standard_viz_dynamic":
        return pickle.loads(row["value"])
    return row["value"]


def get_project_table(engine, project_name: str) -> pd.DataFrame:
    data = get_all_values(engine, project_name)
    data = data.drop("valueID", axis=1)
    data.value = data.apply(lambda r: _unpickle(r), axis=1)
    return data
