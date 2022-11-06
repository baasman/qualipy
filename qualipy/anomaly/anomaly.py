import pandas as pd
from qualipy.anomaly.generate import GenerateAnomalies
import qualipy as qpy


def anomaly_data_project(project, retrain):
    generator = GenerateAnomalies(project)
    rule_anomalies = generator.create_trend_rule_table()
    boolean_checks = generator.create_error_check_table()
    cat_anomalies = generator.create_anom_cat_table(retrain)
    num_anomalies = generator.create_anom_num_table(retrain)
    anomalies = pd.concat(
        [num_anomalies, cat_anomalies, boolean_checks, rule_anomalies]
    )
    return anomalies


def run_anomaly(
    project: qpy.Project,
    retrain: bool = False,
    clear_existing: bool = False,
) -> pd.DataFrame:
    anom_data = anomaly_data_project(
        project=project,
        retrain=retrain,
    )
    project.write_anomalies(anomaly_data=anom_data, clear=clear_existing)
    return anom_data
