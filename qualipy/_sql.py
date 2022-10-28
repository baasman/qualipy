import pandas as pd
from sqlalchemy import delete, engine
import sqlalchemy as sa

from typing import Callable, List, Union
import typing as t

from sqlalchemy.sql.expression import null


# back compatibility purposes
value_id_name = "value_id"


class SQL:
    def __init__(self, engine, schema: str = None):
        self.engine = engine
        self.schema = schema
        self.meta = sa.MetaData(bind=self.engine, schema=schema)

    def create_table(self, table_name: str, extend_existing: bool = False) -> None:
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
                sa.Column("meta", sa.String, nullable=True),
                extend_existing=extend_existing,
            )
            self.meta.create_all()

    def create_anomaly_table(
        self, table_name: str, extend_existing: bool = False
    ) -> None:
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
                sa.Column("arguments", sa.String, nullable=True),
                sa.Column("return_format", sa.String, nullable=False, default="float"),
                sa.Column("trend_function_name", sa.String, nullable=True),
                sa.Column("batch_name", sa.String, nullable=False),
                sa.Column("run_name", sa.String, nullable=True),
                sa.Column("value", sa.String, nullable=True),
                sa.Column("severity", sa.String, nullable=True),
                sa.Column("insert_time", sa.DateTime, nullable=False),
                extend_existing=extend_existing,
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
        data = pd.read_sql(query, con=self.engine)
        return data

    def does_table_exist(self, table_name: str) -> bool:
        with self.engine.connect() as conn:
            exists = conn.execute(
                "select name from sqlite_master "
                'where type="table" '
                'and name="{}"'.format(table_name)
            ).fetchone()
        return exists

    def delete_existing_batch(self, trans, batch_name):
        delete_query = sa.delete(self.table).where(
            self.table.c.batch_name == str(batch_name)
        )
        trans.execute(delete_query)

    def does_batch_exist(self, batch_name):
        with self.engine.connect() as conn:
            query = (
                sa.select(self.table)
                .where(self.table.c.batch_name == str(batch_name))
                .limit(1)
            )
            exists = conn.execute(query).fetchone()
        if exists:
            return True
        return False


class SQLite(SQL):
    def __init__(self, engine, schema):
        if schema is not None:
            raise ValueError
        super(SQLite, self).__init__(engine, schema)


class Postgres(SQL):
    def __init__(self, engine, schema):
        super(Postgres, self).__init__(engine, schema)

    def does_table_exist(self, table_name: str) -> bool:
        schema = "public" if self.schema is None else self.schema
        with self.engine.connect() as conn:
            exists = conn.execute(
                f"SELECT to_regclass('{schema}.{table_name}')"
            ).fetchone()
        return exists[0]

    def create_schema_if_not_exists(self) -> None:
        if self.schema is not None:
            with self.engine.connect() as conn:
                conn.execute(
                    f"create schema if not exists {self.schema} authorization postgres"
                )


DB_ENGINES = {"sqlite": SQLite, "postgres": Postgres}
