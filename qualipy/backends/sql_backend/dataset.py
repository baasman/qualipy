from qualipy.backends.base import BaseData

import sqlalchemy as sa

import uuid


class SQLData(BaseData):
    def __init__(
        self,
        engine=None,
        table_name=None,
        schema=None,
        conn_string=None,
        custom_select_sql=None,
    ):
        if engine is not None:
            self.engine = engine
        else:
            self.engine = sa.create_engine(conn_string)
        self.dialect = self.engine.dialect.name.lower()
        if custom_select_sql is not None:
            if table_name is None:
                table_name = str(uuid.uuid4())[:8]
            if self.dialect == "mssql":
                table_name = "#" + table_name
            else:
                table_name = f"tmp_{table_name}"
            self._create_temp_table(table_name, custom_select_sql, schema)
        self.table_name = table_name

        self._table = sa.Table(table_name, sa.MetaData(), schema=schema)
        insp = sa.engine.reflection.Inspector.from_engine(self.engine)
        self.table_reflection = insp.get_columns(table_name, schema=schema)
        self.custom_where = None

    def get_data(self):
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
                    IF NOT EXISTS {schema}{table_name} 
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

    def set_custom_where(self, custom_where):
        self.custom_where = custom_where

    def _table_exists_fallback(self, table_name):
        try:
            self.engine.execute(
                sa.select([sa.text("*")]).select_from(sa.text(table_name))
            )
        except:
            return False
        return True
