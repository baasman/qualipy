import pandas as pd
from sqlalchemy import engine

from typing import Callable, List, Union
import pickle
import datetime


# back compatibility purposes
value_id_name = "value_id"


def _unpickle(row):
    if row["return_format"] == "dict":
        return pickle.loads(row["value"])
    return row["value"]


# TODO: take as input schema
class SQL:
    def create_table(self, engine: engine.base.Engine, table_name: str) -> None:
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
                "run_name" CHARACTER not null DEFAULT FALSE,
                "value_id" CHARACTER(36) null,
                "insert_time" DATETIME not null
            );
        """.format(
            table_name
        )
        exists = self.does_table_exist(engine, table_name)
        if not exists:
            with engine.connect() as conn:
                conn.execute(create_table_query)

    def create_anomaly_table(self, engine: engine.base.Engine, table_name: str) -> None:
        create_table_query = """
            create table {} (
                "project" CHARACTER(30) not null,
                "column_name" CHARACTER(30) not null,
                "date" DATETIME not null,
                "metric" CHARACTER(30) not null,
                "arguments" CHARACTER(100) null,
                "return_format" CHARACTER DEFAULT 'float',
                "batch_name" CHARACTER null DEFAULT true,
                "run_name" CHARACTER not null DEFAULT FALSE,
                "value" CHARACTER(36) null,
                "insert_time" DATETIME not null
            );
        """.format(
            table_name
        )
        exists = self.does_table_exist(engine, table_name)
        if not exists:
            with engine.connect() as conn:
                conn.execute(create_table_query)

    def create_value_table(self, engine: engine.base.Engine, table_name: str) -> None:
        create_table_query = """
            create table {} (
                value varchar,
                value_id CHARACTER(36),
                    foreign key (value_id) references {}(value_id)
            );
        """.format(
            table_name, table_name.replace("_values", "")
        )
        exists = self.does_table_exist(engine, table_name)
        if not exists:
            with engine.connect() as conn:
                conn.execute(create_table_query)

    def create_custom_value_table(
        self, engine: engine.base.Engine, table_name: str
    ) -> None:
        create_table_query = """
            create table {} (
                value BLOB,
                value_id CHARACTER(36),
                    foreign key (value_id) references {}(value_id)
            );
        """.format(
            table_name, table_name.replace("_values_custom", "")
        )
        exists = self.does_table_exist(engine, table_name)
        if not exists:
            with engine.connect() as conn:
                conn.execute(create_table_query)

    def get_table(self, engine: engine.base.Engine, table_name: str) -> pd.DataFrame:
        return pd.read_sql("select * from {}".format(table_name), engine)

    def get_all_values(
        self, engine: engine.base.Engine, table_name: str, last_date: str = None
    ) -> pd.DataFrame:
        value_table = table_name + "_values"
        # for back compatibility purposes
        if last_date is not None:
            where_stmt = f"where insert_time > '{last_date}'"
        else:
            where_stmt = ""
        query = f"""
        select *
        from {table_name}
        join (select * from {value_table} UNION select * from {table_name + '_values_custom'}) as {value_table + '_all'}
        on {table_name}.{value_id_name} = {value_table + '_all'}.{value_id_name}
        {where_stmt};
        """
        return pd.read_sql(query, engine)

    def delete_data(
        self, conn: engine.base.Connection, name: str, create_function: Callable
    ) -> None:
        try:
            conn.execute("drop table {}".format(name))
        except:
            pass
        self.create_table_if_not_exists(conn, name, create_function)

    def create_table_if_not_exists(
        self, conn: engine.base.Connection, name: str, create_function: Callable
    ) -> None:
        exists = conn.execute(
            "select name from sqlite_master "
            'where type="table" '
            'and name="{}"'.format(name)
        ).fetchone()
        if not exists:
            create_function(conn, name)

    def get_project_table(
        self, engine, project_name: str, last_date: str = None
    ) -> pd.DataFrame:
        data = self.get_all_values(engine, project_name, last_date)
        data = data.drop(value_id_name, axis=1)
        if data.shape[0] > 0:
            data.value = data.apply(lambda r: _unpickle(r), axis=1)
        return data

    def get_anomaly_table(self, engine, project_name: str) -> pd.DataFrame:
        anom_table_name = f"{project_name}_anomaly"
        data = pd.read_sql(f"select * from {anom_table_name}", con=engine)
        return data

    def get_last_time(self, engine, project_name: str):
        with engine.connect() as conn:
            time = conn.execute(
                f"select insert_time from {project_name} order by rowid desc limit 1"
            ).fetchone()[0]
            time = datetime.datetime.strptime(time.split(".")[0], "%Y-%m-%d %H:%M:%S")
        return time

    def does_table_exist(self, engine: engine.base.Engine, table_name: str) -> bool:
        with engine.connect() as conn:
            exists = conn.execute(
                "select name from sqlite_master "
                'where type="table" '
                'and name="{}"'.format(table_name)
            ).fetchone()
        return exists

    def get_top_row(
        self,
        engine: engine.base.Engine,
        variables: Union[str, List[str]],
        table_name: str,
    ):
        if variables == "all":
            var_selection = "*"
        else:
            var_selection = ",".join(variables)
        query = f"select {var_selection} from {table_name} limit 1"
        with engine.connect() as conn:
            row = pd.read_sql(query, conn)
        return row


class SQLite(SQL):
    def __init__(self):
        super(SQLite).__init__()


class Postgres(SQL):
    def __init__(self):
        super(Postgres).__init__()

    def create_table(self, engine: engine.base.Engine, table_name: str) -> None:
        create_table_query = """
            create table {} (
                "id" SERIAL,
                "column_name" VARCHAR(100) not null,
                "date" TIMESTAMPTZ not null,
                "metric" VARCHAR(100) not null,
                "arguments" VARCHAR(100) null,
                "type" VARCHAR(50) not null DEFAULT 'custom',
                "return_format" VARCHAR(10) DEFAULT 'float',
                "standard_viz" VARCHAR(100) null,
                "is_static" BOOLEAN null DEFAULT true,
                "key_function" BOOLEAN null DEFAULT FALSE,
                "batch_name" VARCHAR(100) null DEFAULT true,
                "run_name" VARCHAR(100) not null DEFAULT FALSE,
                "value_id" VARCHAR(36) null, 
                "insert_time" TIMESTAMPTZ not null, 
                PRIMARY KEY (id, value_id),
                UNIQUE (value_id) 
            );
        """.format(
            table_name
        )
        exists = self.does_table_exist(engine, table_name)
        if not exists:
            with engine.connect() as conn:
                conn.execute(create_table_query)

    def create_value_table(self, engine: engine.base.Engine, table_name: str) -> None:
        create_table_query = """
            create table {} (
                value varchar,
                value_id VARCHAR(36),
                    foreign key (value_id) references {}(value_id) on delete cascade
            );
        """.format(
            table_name, table_name.replace("_values", "")
        )
        exists = self.does_table_exist(engine, table_name)
        if not exists:
            with engine.connect() as conn:
                conn.execute(create_table_query)

    def create_custom_value_table(
        self, engine: engine.base.Engine, table_name: str
    ) -> None:
        create_table_query = """
            create table {} (
                value BYTEA,
                value_id VARCHAR(36),
                    foreign key (value_id) references {}(value_id) on delete cascade
            );
        """.format(
            table_name, table_name.replace("_values_custom", "")
        )
        exists = self.does_table_exist(engine, table_name)
        if not exists:
            with engine.connect() as conn:
                conn.execute(create_table_query)

    def create_anomaly_table(self, engine: engine.base.Engine, table_name: str) -> None:
        create_table_query = """
            create table {} (
                "project" VARCHAR(50) not null,
                "column_name" VARCHAR(100) not null,
                "date" TIMESTAMPTZ not null,
                "metric" VARCHAR(100) not null,
                "arguments" VARCHAR(100) null,
                "return_format" VARCHAR(10) DEFAULT 'float',
                "batch_name" VARCHAR(100) null DEFAULT true,
                "run_name" VARCHAR(100) not null DEFAULT FALSE,
                "value" VARCHAR(36) null,
                "insert_time" TIMESTAMPTZ not null
            );
        """.format(
            table_name
        )
        exists = self.does_table_exist(engine, table_name)
        if not exists:
            with engine.connect() as conn:
                conn.execute(create_table_query)

    def does_table_exist(self, engine: engine.base.Engine, table_name: str) -> bool:
        with engine.connect() as conn:
            exists = conn.execute(
                f"SELECT to_regclass('public.{table_name}')"
            ).fetchone()
        return exists[0]

    def delete_data(
        self, conn: engine.base.Connection, name: str, create_function: Callable
    ) -> None:
        try:
            conn.execute("drop table {} cascade".format(name))
        except:
            pass
        self.create_table_if_not_exists(conn, name, create_function)

    def create_table_if_not_exists(
        self, conn: engine.base.Connection, name: str, create_function: Callable
    ) -> None:
        exists = conn.execute(f"SELECT to_regclass('public.{name}')").fetchone()[0]
        if not exists:
            create_function(conn, name)

    def get_all_values(
        self, engine: engine.base.Engine, table_name: str, last_date: str = None
    ) -> pd.DataFrame:
        value_table = table_name + "_values"
        if last_date is not None:
            where_stmt = f"where insert_time > '{last_date}'"
        else:
            where_stmt = ""
        query_non_binary = f"""
            select *
            from {table_name}
            join {value_table} on 
                {table_name}.value_id = {value_table}.value_id
        """
        non_bin_data = pd.read_sql(query_non_binary, engine, index_col="id")
        query_binary = f"""
            select *
            from {table_name}
            join {value_table + '_custom'} on 
                {table_name}.value_id = {value_table + '_custom'}.value_id
        """
        bin_data = pd.read_sql(query_binary, engine, index_col="id")
        data = pd.concat([non_bin_data, bin_data]).reset_index(drop=True)
        return data


DB_ENGINES = {"sqlite": SQLite, "postgresql": Postgres}

if __name__ == "__main__":
    from sqlalchemy import create_engine

    p = Postgres()
    p.create_table(
        create_engine("postgresql+psycopg2://postgres:docker@127.0.0.1:5433/postgres"),
        "test_table",
    )
    p.create_value_table(
        create_engine("postgresql+psycopg2://postgres:docker@127.0.0.1:5433/postgres"),
        "test_table_values",
    )
