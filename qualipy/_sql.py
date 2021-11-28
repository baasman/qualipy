import pandas as pd
from sqlalchemy import engine
import sqlalchemy as sa

from typing import Callable, List, Union
import pickle
import datetime

from sqlalchemy.sql.expression import null


# back compatibility purposes
value_id_name = "value_id"


class SQL:
    def __init__(self, engine, schema: str = None):
        self.engine = engine
        self.schema = schema
        self.meta = sa.MetaData(bind=self.engine)

    def create_table(self, table_name: str) -> None:
        exists = self.does_table_exist(table_name)
        if not exists:
            table = sa.Table(
                table_name,
                self.meta,
                sa.Column("id", sa.Integer, autoincrement=True, primary_key=True),
                sa.Column("column_name", sa.String, nullable=False),
                sa.Column("date", sa.DateTime, nullable=False),
                sa.Column("metric", sa.String, nullable=False),
                sa.Column("arguments", sa.String, nullable=True),
                sa.Column("type", sa.String, nullable=False, default="custom"),
                sa.Column("return_format", sa.String, nullable=False, default="float"),
                sa.Column("batch_name", sa.String, nullable=False),
                sa.Column("run_name", sa.String, nullable=False),
                sa.Column("value", sa.String, nullable=True),
                sa.Column("insert_time", sa.DateTime, nullable=False),
            )
            self.meta.create_all()

    def create_anomaly_table(self, table_name: str) -> None:
        exists = self.does_table_exist(table_name)
        if not exists:
            table = sa.Table(
                table_name,
                self.meta,
                sa.Column("id", sa.Integer, autoincrement=True, primary_key=True),
                sa.Column("project", sa.String, nullable=False),
                sa.Column("column_name", sa.String, nullable=False),
                sa.Column("date", sa.DateTime, nullable=False),
                sa.Column("metric", sa.String, nullable=False),
                sa.Column("arguments", sa.String, nullable=False),
                sa.Column("trend_function_name", sa.String, nullable=True),
                sa.Column("batch_name", sa.String, nullable=False),
                sa.Column("run_name", sa.String, nullable=False),
                sa.Column("value", sa.String, nullable=True),
                sa.Column("severity", sa.String, nullable=True),
                sa.Column("insert_time", sa.DateTime, nullable=False),
            )
            self.meta.create_all()

    def reflect_tables(self, main_table_name, anomaly_table_name):
        table = sa.Table(
            main_table_name,
            self.meta,
            autoload_with=self.engine,
            schema=self.schema,
        )
        anomaly_table = sa.Table(
            anomaly_table_name,
            self.meta,
            autoload_with=self.engine,
            schema=self.schema,
        )
        self.table = table
        self.anomaly_table = anomaly_table

    def create_schema_if_not_exists(self) -> None:
        pass

    def get_all_values(self, last_date: str = None) -> pd.DataFrame:
        query = sa.select("*").select_from(self.table)

        if last_date is not None:
            where_stmt = self.table.c["insert_time"] > last_date
            query = getattr(query, "where")(where_stmt)

        data = pd.read_sql(query, self.engine)
        data.date = pd.to_datetime(data.date)
        return data

    def delete_data(
        self,
        name: str,
        anomaly_name: str,
        recreate: bool = False,
    ) -> None:
        with self.engine.begin():
            self.table.drop()
            self.anomaly_table.drop()
        if recreate:
            self.create_table(name)
            self.create_anomaly_table(anomaly_name)
            self.reflect_tables(name, anomaly_name)

    def get_project_table(self, last_date: str = None) -> pd.DataFrame:
        data = self.get_all_values(last_date)
        return data

    def get_anomaly_table(self) -> pd.DataFrame:
        query = sa.select("*").select_from(self.anomaly_table)
        data = pd.read_sql(query, con=engine)
        return data

    def does_table_exist(self, table_name: str) -> bool:
        with self.engine.connect() as conn:
            exists = conn.execute(
                "select name from sqlite_master "
                'where type="table" '
                'and name="{}"'.format(table_name)
            ).fetchone()
        return exists


class SQLite(SQL):
    def __init__(self, engine, schema):
        if schema is not None:
            raise ValueError
        super(SQLite, self).__init__(engine, schema)


class Postgres(SQL):
    def __init__(self, schema):
        # TODO: refactor at some point - totally broken now
        raise NotImplementedError
        super(Postgres, self).__init__(schema)

    def create_table(self, table_name: str) -> None:
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
                PRIMARY KEY (id)
            );
        """
        exists = self.does_table_exist(engine, table_name)
        if not exists:
            with engine.connect() as conn:
                conn.execute(create_table_query)

    def create_anomaly_table(self, table_name: str) -> None:
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


DB_ENGINES = {"sqlite": SQLite}
