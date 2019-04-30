import pandas as pd


def create_table(engine, table_name):
    create_table_query = '''
        create table {} (
            "_name" CHARACTER(20) not null,
            "_date" DATETIME not null,
            "_metric" CHARACTER(30) not null,
            "_arguments" CHARACTER(100) null,
            "value" BLOB,
            "_type" CHARACTER not null DEFAULT 'custom',
            "_standard_viz" CHARACTER(100) null,
            "_over_time" BOOLEAN null DEFAULT true
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
