from qualipy.project import Project

from sklearn.ensemble import IsolationForest
import joblib
import numpy as np
import pandas as pd

import os
import warnings
import traceback


mods = {"IsolationForest": IsolationForest}
default_config_path = os.path.join(os.path.expanduser("~"), ".qualipy")


def create_file_name(model_dir, project_name, col_name, metric_name, arguments):
    file_name = os.path.join(
        model_dir, f"{project_name}_{col_name}_{metric_name}_{arguments}.mod"
    )
    return file_name


class AnomalyModel(object):
    def __init__(
        self, model="IsolationForest", args=None, config_loc=default_config_path
    ):
        self.args = (
            {"behaviour": "new", "contamination": 0.03, "n_estimators": 50}
            if args is None
            else args
        )
        self.anom_model = mods[model](**self.args)
        self.model_dir = os.path.join(config_loc, "models")
        if not os.path.isdir(self.model_dir):
            os.mkdir(self.model_dir)

    def train(self, train_data):
        self.anom_model.fit(train_data)

    def save(self, project_name, col_name, metric_name, arguments=None):
        file_name = create_file_name(
            self.model_dir, project_name, col_name, metric_name, arguments
        )
        print(f"Writing anomaly model to {file_name}")
        joblib.dump(self.anom_model, file_name)


class LoadedModel(object):
    def __init__(self, config_loc=default_config_path):
        self.model_dir = os.path.join(config_loc, "models")

    def load(self, project_name, col_name, metric_name, arguments=None):
        file_name = create_file_name(
            self.model_dir, project_name, col_name, metric_name, arguments
        )
        print(f"Loading model from {file_name}")
        self.anom_model = joblib.load(file_name)

    def predict(self, test_data):
        return self.anom_model.predict(test_data)


class RunModels(object):
    def __init__(self, project_name, engine, config_dir=default_config_path):
        self.config_dir = config_dir
        self.project = Project(project_name, engine, config_dir=config_dir)

    def train_all(self):
        df = self.project.get_project_table()
        df = df[
            (df["type"] == "numerical") | (df["column_name"].isin(["rows", "columns"]))
        ]
        df.value = df.value.astype(float)
        df["metric_name"] = (
            df.column_name
            + "_"
            + df.metric.astype(str)
            + "_"
            + np.where(df.arguments.isnull(), "", df.arguments)
        )
        for metric_name, data in df.groupby("metric_name"):
            print(metric_name)
            mod = AnomalyModel(config_loc=self.config_dir)
            try:
                mod.train(data.value.values.reshape((-1, 1)))
                mod.save(
                    self.project.project_name,
                    data.column_name.values[0],
                    data.metric.values[0],
                    data.arguments.values[0],
                )
            except ValueError:
                warnings.warn(f"Unable to create anomaly model for {metric_name}")


class GenerateAnomalies(object):
    def __init__(self, project_name, engine, config_dir=default_config_path):
        self.config_dir = config_dir
        self.project = Project(project_name, engine, config_dir=config_dir)

    def create_anom_num_table(self):
        df = self.project.get_project_table()
        df = df[
            (df["type"] == "numerical") | (df["column_name"].isin(["rows", "columns"]))
        ]
        df.value = df.value.astype(float)
        df["metric_name"] = (
            df.column_name
            + "_"
            + df.metric.astype(str)
            + "_"
            + np.where(df.arguments.isnull(), "", df.arguments)
        )
        all_rows = []
        for metric_name, data in df.groupby("metric_name"):
            print(metric_name)
            try:
                mod = LoadedModel(config_loc=self.config_dir)
                mod.load(
                    self.project.project_name,
                    data.column_name.values[0],
                    data.metric.values[0],
                    data.arguments.values[0],
                )
                preds = mod.predict(data.value.values.reshape((-1, 1)))
                outlier_rows = data[preds == -1]
                if outlier_rows.shape[0] > 0:
                    all_rows.append(outlier_rows)
            except ValueError:
                warnings.warn(f"Unable to load anomaly model for {metric_name}")
            except FileNotFoundError:
                warnings.warn(f"Unable to load anomaly model for {metric_name}")
        data = pd.concat(all_rows).sort_values("date", ascending=False)
        data = data[
            ["column_name", "date", "metric", "arguments", "value", "batch_name"]
        ]
        return data


if __name__ == "__main__":
    from sqlalchemy import create_engine

    engine = create_engine("sqlite:////data/baasman/qualipy_dbs/test.db")
    g = GenerateAnomalies("pat_enc", engine, config_dir="/home/baasman/.qualipy")
    rows = g.create_anom_num_table()
    print(rows)
