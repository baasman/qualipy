import dash_table
from sqlalchemy import create_engine
import pandas as pd

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
    try:
        num_anomalies = generator.create_anom_num_table()
        cat_anomalies = generator.create_anom_cat_table()
        anomalies = pd.concat([num_anomalies, cat_anomalies]).sort_values(
            "date", ascending=False
        )
    except ValueError:
        anomalies = pd.DataFrame(
            [],
            columns=[
                "column_name",
                "date",
                "metric",
                "arguments",
                "value",
                "batch_name",
            ],
        )
    return anomalies


def anomaly_num_table(num_anomalies):
    return dash_table.DataTable(
        id="schema-table",
        columns=[{"name": i, "id": i} for i in num_anomalies.columns],
        data=num_anomalies.to_dict("rows"),
        sort_action="native",
        style_cell={
            "overflow": "hidden",
            "textOverflow": "ellipsis",
            "maxWidth": "250px",
        },
    )
