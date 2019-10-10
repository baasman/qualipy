import pandas as pd
from sqlalchemy import engine

from typing import Callable
import pickle
import datetime


def _unpickle(row):
    if row["return_format"] == "dict":
        return pickle.loads(row["value"])
    return row["value"]


class SQL:
    @staticmethod
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
                "valueID" CHARACTER(36) null,
                "insert_time" DATETIME not null
            );
        """.format(
            table_name
        )
        conn.execute(create_table_query)

    @staticmethod
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

    @staticmethod
    def create_custom_value_table(
        conn: engine.base.Connection, table_name: str
    ) -> None:
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

    @staticmethod
    def get_table(engine: engine.base.Engine, table_name: str) -> pd.DataFrame:
        return pd.read_sql("select * from {}".format(table_name), engine)

    @staticmethod
    def get_all_values(
        engine: engine.base.Engine, table_name: str, last_date: str = None
    ) -> pd.DataFrame:
        value_table = table_name + "_values"
        if last_date is not None:
            where_stmt = f"where insert_time > '{last_date}'"
        else:
            where_stmt = ""
        query = f"""
        select *
        from {table_name}
        join (select * from {value_table} UNION select * from {table_name + '_values_custom'}) as {value_table + '_all'}
        on {table_name}.valueID = {value_table + '_all'}.valueID
        {where_stmt};
        """
        return pd.read_sql(query, engine)

    @staticmethod
    def delete_data(
        conn: engine.base.Connection, name: str, create_function: Callable
    ) -> None:
        try:
            conn.execute("drop table {}".format(name))
        except:
            pass
        SQL.create_table_if_not_exists(conn, name, create_function)

    @staticmethod
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

    @staticmethod
    def get_project_table(
        engine, project_name: str, last_date: str = None
    ) -> pd.DataFrame:
        data = SQL.get_all_values(engine, project_name, last_date)
        data = data.drop("valueID", axis=1)
        if data.shape[0] > 0:
            data.value = data.apply(lambda r: _unpickle(r), axis=1)
        return data

    @staticmethod
    def get_last_time(engine, project_name: str):
        with engine.connect() as conn:
            time = conn.execute(
                f"select insert_time from {project_name} order by rowid desc limit 1"
            ).fetchone()[0]
            time = datetime.datetime.strptime(time.split(".")[0], "%Y-%m-%d %H:%M:%S")
        return time

    @staticmethod
    def get_top_row(engine, variables, table_name):
        if variables == "all":
            var_selection = "*"
        else:
            var_selection = ",".join(variables)
        query = f"select {var_selection} from {table_name} limit 1"
        with engine.connect() as conn:
            row = pd.read_sql(query, conn)


class SQLite(SQL):
    pass
