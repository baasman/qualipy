import pandas as pd


def create_table(engine, table_name):
    create_table_query = '''
        create table {} (
            "column_name" CHARACTER(20) not null,
            "date" DATETIME not null,
            "metric" CHARACTER(30) not null,
            "arguments" CHARACTER(100) null,
            "value" BLOB,
            "type" CHARACTER not null DEFAULT 'custom',
            "standard_viz" CHARACTER(100) null,
            "is_static" BOOLEAN null DEFAULT true,
            "batch_name" CHARACTER null DEFAULT true
        );
    '''.format(table_name)
    with engine.connect() as conn:
        conn.execute(create_table_query)


def create_alert_table(engine, table_name):
    create_table_query = '''
        create table {} (
            "column" CHARACTER(20) not null,
            "std_away" NUMERIC not null,
            "value" NUMERIC not null,
            "alert_message" CHARACTER(100) null,
            "date" DATETIME not null
        );
    '''.format(table_name)
    with engine.connect() as conn:
        conn.execute(create_table_query)

def get_table(engine, table_name):
    return pd.read_sql('select * from {}'.format(table_name), engine)
