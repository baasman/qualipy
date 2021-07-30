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
    def __init__(self, schema: str = None):
        self.schema = schema

    def create_table(self, engine: engine.base.Engine, table_name: str) -> None:
        create_table_query = """
            create table {} (
                "id" INTEGER primary key autoincrement,
                "column_name" CHARACTER not null,
                "date" DATETIME not null,
                "metric" CHARACTER not null,
                "arguments" CHARACTER null,
                "type" CHARACTER not null DEFAULT 'custom',
                "return_format" CHARACTER DEFAULT 'float',
                "batch_name" CHARACTER null DEFAULT true,
                "run_name" CHARACTER not null DEFAULT FALSE,
                "value" CHARACTER null,
                "insert_time" DATETIME not null,
                "valid_min" CHARACTER null,
                "valid_max" CHARACTER null
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
                "id" INTEGER primary key autoincrement,
                "project" CHARACTER not null,
                "column_name" CHARACTER not null,
                "date" DATETIME not null,
                "metric" CHARACTER not null,
                "arguments" CHARACTER null,
                "trend_function_name" CHARACTER null,
                "return_format" CHARACTER DEFAULT 'float',
                "batch_name" CHARACTER null DEFAULT true,
                "run_name" CHARACTER not null DEFAULT FALSE,
                "value" CHARACTER null,
                "severity" CHARACTER null,
                "insert_time" DATETIME not null,
                "valid_min" CHARACTER null,
                "valid_max" CHARACTER null
            );
        """.format(
            table_name
        )
        exists = self.does_table_exist(engine, table_name)
        if not exists:
            with engine.connect() as conn:
                conn.execute(create_table_query)

    def create_schema_if_not_exists(self, engine) -> None:
        pass

    def get_all_values(
        self, engine: engine.base.Engine, table_name: str, last_date: str = None
    ) -> pd.DataFrame:
        if last_date is not None:
            where_stmt = f"where insert_time > '{last_date}'"
        else:
            where_stmt = ""
        query = f"""
            select * from {table_name}
        """
        data = pd.read_sql(query, engine)
        data.date = pd.to_datetime(data.date)
        return data

    def delete_data(
        self,
        engine: engine.base.Engine,
        name: str,
        anomaly_name: str,
        recreate: bool = False,
    ) -> None:
        schema = self.schema + "." if self.schema is not None else ""
        with engine.begin() as conn:
            conn.execute(f"drop table {schema}{name}")
            conn.execute(f"drop table {schema}{anomaly_name}")
        if recreate:
            self.create_table(engine, name)
            self.create_anomaly_table(engine, anomaly_name)

    def get_project_table(
        self, engine, project_name: str, last_date: str = None
    ) -> pd.DataFrame:
        data = self.get_all_values(engine, project_name, last_date)
        return data

    def get_anomaly_table(self, engine, project_name: str) -> pd.DataFrame:
        anom_table_name = f"{project_name}_anomaly"
        data = pd.read_sql(f"select * from {anom_table_name}", con=engine)
        return data

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
    def __init__(self, schema):
        # TODO: cant have schema in sqlite
        if schema is not None:
            raise ValueError
        super(SQLite, self).__init__(schema)


class Postgres(SQL):
    def __init__(self, schema):
        super(Postgres, self).__init__(schema)

    def create_table(self, engine: engine.base.Engine, table_name: str) -> None:
        schema = self.schema + "." if self.schema is not None else ""
        create_table_query = f"""
            create table {schema}{table_name} (
                "id" SERIAL,
                "column_name" VARCHAR(100) not null,
                "date" TIMESTAMPTZ not null,
                "metric" VARCHAR(100) not null,
                "arguments" VARCHAR null,
                "type" VARCHAR(50) not null DEFAULT 'custom',
                "return_format" VARCHAR(10) DEFAULT 'float',
                "batch_name" VARCHAR(100) null DEFAULT true,
                "run_name" VARCHAR(100) not null DEFAULT FALSE,
                "value" VARCHAR null, 
                "insert_time" TIMESTAMPTZ not null, 
                "valid_min" VARCHAR null, 
                "valid_max" VARCHAR null, 
                PRIMARY KEY (id)
            );
        """
        exists = self.does_table_exist(engine, table_name)
        if not exists:
            with engine.connect() as conn:
                conn.execute(create_table_query)

    def create_anomaly_table(self, engine: engine.base.Engine, table_name: str) -> None:
        schema = self.schema + "." if self.schema is not None else ""
        create_table_query = f"""
            create table {schema}{table_name} (
                "id" SERIAL,
                "project" VARCHAR(50) not null,
                "column_name" VARCHAR(100) not null,
                "date" TIMESTAMPTZ not null,
                "metric" VARCHAR(100) not null,
                "arguments" VARCHAR null,
                "return_format" VARCHAR(10) DEFAULT 'float',
                "batch_name" VARCHAR(100) null DEFAULT true,
                "run_name" VARCHAR(100) not null DEFAULT FALSE,
                "value" VARCHAR null,
                "severity" VARCHAR null,
                "insert_time" TIMESTAMPTZ not null,
                "valid_min" VARCHAR null, 
                "valid_max" VARCHAR null,
                "trend_function_name" VARCHAR null,
                PRIMARY KEY (id)
            );
        """
        exists = self.does_table_exist(engine, table_name)
        if not exists:
            with engine.connect() as conn:
                conn.execute(create_table_query)

    def does_table_exist(self, engine: engine.base.Engine, table_name: str) -> bool:
        schema = "public" if self.schema is None else self.schema
        with engine.connect() as conn:
            exists = conn.execute(
                f"SELECT to_regclass('{schema}.{table_name}')"
            ).fetchone()
        return exists[0]

    def delete_data(
        self,
        engine: engine.base.Engine,
        name: str,
        anomaly_name: str,
        recreate: bool = False,
    ) -> None:
        schema = self.schema + "." if self.schema is not None else ""
        with engine.begin() as conn:
            conn.execute(f"drop table {schema}{name}")
            conn.execute(f"drop table {schema}{anomaly_name}")
        if recreate:
            self.create_table(engine, name)
            self.create_anomaly_table(engine, anomaly_name)

    def create_schema_if_not_exists(self, engine) -> None:
        if self.schema is not None:
            with engine.connect() as conn:
                conn.execute(
                    f"create schema if not exists {self.schema} authorization postgres"
                )

    def get_all_values(
        self, engine: engine.base.Engine, table_name: str, last_date: str = None
    ) -> pd.DataFrame:
        schema = self.schema + "." if self.schema is not None else ""
        if last_date is not None:
            where_stmt = f"where insert_time > '{last_date}'"
        else:
            where_stmt = ""
        query = f"""
            select *
            from {schema}{table_name}
        """
        data = pd.read_sql(query, engine, index_col="id")
        # check this later
        data.date = pd.to_datetime(data.date, utc=True)
        return data

    def get_anomaly_table(self, engine, project_name: str) -> pd.DataFrame:
        schema = self.schema + "." if self.schema is not None else ""
        anom_table_name = f"{project_name}_anomaly"
        data = pd.read_sql(f"select * from {schema}{anom_table_name}", con=engine)
        return data


DB_ENGINES = {"sqlite": SQLite, "postgresql": Postgres}
