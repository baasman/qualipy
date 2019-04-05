import pandas as pd


def create_table(engine, table_name):
    create_table_query = '''
        create table {} (
            "_name" CHARACTER(20) not null,
            "_date" DATETIME not null,
            "_metric" CHARACTER(30) not null,
            "value" NUMERIC
        );
    '''.format(table_name)
    with engine.connect() as conn:
        conn.execute(create_table_query)


def get_table(engine, table_name):
    return pd.read_sql('select * from {}'.format(table_name), engine)
