import json
import os

import pandas as pd

from sqlalchemy import create_engine
from qualipy.anomaly.generate import GenerateAnomalies


def anomaly_data_project(project_name, config_dir, retrain):
    generator = GenerateAnomalies(project_name, config_dir)
    boolean_checks = generator.create_error_check_table()
    cat_anomalies = generator.create_anom_cat_table(retrain)
    num_anomalies = generator.create_anom_num_table(retrain)
    anomalies = pd.concat([num_anomalies, cat_anomalies, boolean_checks]).sort_values(
        "date", ascending=False
    )
    anomalies["project"] = project_name
    anomalies = anomalies[
        ["project"] + [col for col in anomalies.columns if col != "project"]
    ]
    return anomalies


def write_anomaly(conn, data, project_name, clear=False, schema=None):
    schema_str = schema + "." if schema is not None else ""
    anomaly_table_name = f"{project_name}_anomaly"
    if clear:
        conn.execute(
            f"delete from {schema_str}{anomaly_table_name} where project = '{project_name}'"
        )
    most_recent_one = conn.execute(
        f"select date from {schema_str}{anomaly_table_name} order by date desc limit 1"
    ).fetchone()
    if most_recent_one is not None and data.shape[0] > 0:
        most_recent_one = most_recent_one[0]
        data = data[pd.to_datetime(data.date) > pd.to_datetime(most_recent_one)]
    if data.shape[0] > 0:
        data.to_sql(
            anomaly_table_name, conn, if_exists="append", index=False, schema=schema
        )


def _run_anomaly(project_name, config_dir, retrain):
    with open(os.path.join(config_dir, "config.json"), "r") as file:
        loaded_config = json.load(file)
    qualipy_db = loaded_config["QUALIPY_DB"]
    anom_data = anomaly_data_project(
        project_name=project_name, config_dir=config_dir, retrain=retrain,
    )
    engine = create_engine(qualipy_db)
    db_schema = loaded_config.get("SCHEMA")
    with engine.connect() as conn:
        write_anomaly(conn, anom_data, project_name, clear=retrain, schema=db_schema)
