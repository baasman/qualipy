import pandas as pd
import sqlalchemy as sa

from qualipy.anomaly.generate import GenerateAnomalies
import qualipy as qpy
from qualipy.store.initial_models import Value


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
    only_use_batch: str = None,
) -> pd.DataFrame:
    anom_data = anomaly_data_project(
        project=project,
        retrain=retrain,
    )
    if only_use_batch is not None:
        values_to_chose = (
            project.session.query(Value)
            .filter(
                sa.and_(
                    Value.value_id.in_(anom_data.value_id.tolist()),
                    Value.batch_name == only_use_batch,
                )
            )
            .all()
        )
        anom_data = anom_data[anom_data.value_id.isin(values_to_chose)]
    project.write_anomalies(anomaly_data=anom_data, clear=clear_existing)
    return anom_data
