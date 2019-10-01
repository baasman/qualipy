import dash_table
from sqlalchemy import create_engine

from qualipy.anomaly_detection import GenerateAnomalies


def overview_table(data):
    return dash_table.DataTable(
        id="overview-table",
        columns=[{"name": i, "id": i} for i in data.columns],
        data=data.to_dict("rows"),
        sort_action="native",
    )


def schema_table(data):
    return dash_table.DataTable(
        id="schema-table",
        columns=[{"name": i, "id": i} for i in data.columns],
        data=data.to_dict("rows"),
        sort_action="native",
    )


def anomaly_num_data(project_name, db_url, config_dir):
    engine = create_engine(db_url)
    generator = GenerateAnomalies(project_name, engine, config_dir)
    num_anomalies = generator.create_anom_num_table()
    return num_anomalies


def anomaly_num_table(num_anomalies):
    return dash_table.DataTable(
        id="schema-table",
        columns=[{"name": i, "id": i} for i in num_anomalies.columns],
        data=num_anomalies.to_dict("rows"),
        sort_action="native",
    )
