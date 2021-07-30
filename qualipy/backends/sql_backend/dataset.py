from qualipy.backends.base import BaseData
from qualipy.backends.pandas_backend.dataset import PandasData

import sqlalchemy as sa
import pandas as pd

import uuid


class SQLData(BaseData):
    """This is used when tracking a relational table"""

    def __init__(
        self,
        engine: sa.engine.base.Engine = None,
        table_name: str = None,
        schema: str = None,
        conn_string: str = None,
        custom_select_sql: str = None,
        create_temp: bool = False,
        backend="sql",
    ):
        """
        Args:
            engine: A sqlalchemy engine to the database containing the table we want to track
            table_name: The name of the table we want to track
            schema: The schema the table is in
            conn_string: If engine is None, you can just pass the sqlalchemy database connection
            custom_select_sql: Must be proper SQL for whatever DB you are using. This will instantiate
                a temporary table that Qualipy will run against. This is useful if you dont need the
                entire table, or need to run any joins before running Qualipy. However, often it
                might be better to just create a view of what you need.
        """
        if engine is not None:
            self.engine = engine
        else:
            self.engine = sa.create_engine(conn_string)
        self.dialect = self.engine.dialect.name.lower()
        self.custom_select_sql = custom_select_sql
        if custom_select_sql is not None and create_temp:
            if table_name is None:
                table_name = str(uuid.uuid4())[:8]
            if self.dialect == "mssql":
                table_name = "#" + table_name
            else:
                table_name = f"tmp_{table_name}"
            self._create_temp_table(table_name, custom_select_sql, schema)
            if self.dialect != "oracle":
                schema = None
        self.table_name = table_name
        self.schema = schema

        self._table = sa.Table(table_name, sa.MetaData(), schema=schema)
        insp = sa.engine.reflection.Inspector.from_engine(self.engine)
        self.table_reflection = insp.get_columns(table_name, schema=schema)
        self.custom_where = None

        # TODO: implement stratify logic. icluding when converted to pandas
        self.stratify = False
        self.backend = backend

    def get_data(self):
        if self.backend == "pandas":
            if not self.custom_select_sql:
                query = f"select * from {self.table_name}"
            else:
                query = self.custom_select_sql
            data = pd.read_sql(query, self.engine)
            return data
        return self

    def _create_temp_table(self, table_name, sql_statement, schema=None, truncate=True):
        # TODO: definitely not tested with all databases

        # check if exists - if it does, truncate?
        try:
            exists = self.engine.dialect.has_table(
                self.engine, table_name, schema=schema
            )
        except:
            exists = self._table_exists_fallback(table_name)
        schema = schema + "." if schema is not None else ""
        if not exists:
            if self.dialect == "oracle":
                create_query = f"""
                    CREATE GLOBAL TEMPORARY TABLE {schema}{table_name}
                    ON COMMIT PRESERVE ROWS
                    AS ({sql_statement})
                """
            else:
                create_query = f"""
                    CREATE TEMPORARY TABLE
                    IF NOT EXISTS {table_name} 
                    ON COMMIT PRESERVE ROWS
                    AS 
                    ({sql_statement})
                    """
            with self.engine.connect() as conn:
                conn.execute(create_query)
        else:
            if truncate:
                self.engine.execute(f"truncate table {schema}{table_name}")
            with self.engine.connect() as conn:
                conn.execute(f"INSERT INTO {schema}{table_name} ({sql_statement})")

    def _drop_temp_table(self):
        self._table.drop()

    def set_custom_where(self, custom_where: str):
        """Set this when you want a function to run on a subset of the table

        Args:
            custom_where: The where portion of a sql statement. This can then be used in
                a function. See example in the documentation for more information

        """
        self.custom_where = custom_where

    def _table_exists_fallback(self, table_name):
        try:
            self.engine.execute(
                sa.select([sa.text("*")]).select_from(sa.text(table_name))
            )
        except:
            return False
        return True
