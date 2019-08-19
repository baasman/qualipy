import pandas as pd
from sqlalchemy import engine

from typing import Callable


def create_table(engine: engine.base.Engine, table_name: str) -> None:
    create_table_query = """
        create table {} (
            "column_name" CHARACTER(20) not null,
            "date" DATETIME not null,
            "metric" CHARACTER(30) not null,
            "arguments" CHARACTER(100) null,
            "type" CHARACTER not null DEFAULT 'custom',
            "standard_viz" CHARACTER(100) null,
            "is_static" BOOLEAN null DEFAULT true,
            "batch_name" CHARACTER null DEFAULT true,
            "valueID" CHARACTER(36) null 
        );
    """.format(
        table_name
    )
    with engine.connect() as conn:
        conn.execute(create_table_query)


def create_value_table(engine: engine.base.Engine, table_name: str) -> None:
    create_table_query = """
        create table {} (
            value varchar,
            valueID CHARACTER(36),
                foreign key (valueID) references {}(valueID)
        );
    """.format(
        table_name, table_name.replace("_values", "")
    )
    with engine.connect() as conn:
        conn.execute(create_table_query)


def create_custom_value_table(engine: engine.base.Engine, table_name: str) -> None:
    create_table_query = """
        create table {} (
            value_c BLOB,
            valueID CHARACTER(36),
                foreign key (valueID) references {}(valueID)
        );
    """.format(
        table_name, table_name.replace("_values_custom", "")
    )
    with engine.connect() as conn:
        conn.execute(create_table_query)


def create_alert_table(engine: engine.base.Engine, table_name: str) -> None:
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
    with engine.connect() as conn:
        conn.execute(create_table_query)


def get_table(engine: engine.base.Engine, table_name: str) -> pd.DataFrame:
    return pd.read_sql("select * from {}".format(table_name), engine)


def get_all_values(engine: engine.base.Engine, table_name: str) -> pd.DataFrame:
    value_table = table_name + "_values"
    query = f"""
        select *
        from {table_name}
        join {value_table}
        on {table_name}.valueID = {value_table}.valueID
    """
    return pd.read_sql(query, engine)


def delete_data(
    conn: engine.base.Connection, name: str, create_function: Callable
) -> None:
    try:
        conn.execute("drop table {}".format(name))
    except:
        pass
    create_table_if_not_exists(name, create_function)


def create_table_if_not_exists(
    engine: engine.base.Engine, name: str, create_function: Callable
):
    with engine.connect() as conn:
        exists = conn.execute(
            "select name from sqlite_master "
            'where type="table" '
            'and name="{}"'.format(name)
        ).fetchone()
        if not exists:
            create_function(engine, name)
