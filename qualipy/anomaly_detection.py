from qualipy.project import Project

from sklearn.ensemble import IsolationForest
import joblib
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

import os
import warnings
import traceback
from functools import reduce


mods = {"IsolationForest": IsolationForest}
default_config_path = os.path.join(os.path.expanduser("~"), ".qualipy")


def create_file_name(model_dir, project_name, col_name, metric_name, arguments):
    file_name = os.path.join(
        model_dir, f"{project_name}_{col_name}_{metric_name}_{arguments}.mod"
    )
    return file_name


# TODO: this and LoadedModel should be one class
class AnomalyModel(object):
    def __init__(
        self, model="IsolationForest", args=None, config_loc=default_config_path
    ):
        self.args = (
            {"behaviour": "new", "contamination": 0.01, "n_estimators": 50}
            if args is None
            else args
        )
        self.anom_model = mods[model](**self.args)
        self.model_dir = os.path.join(config_loc, "models")
        if not os.path.isdir(self.model_dir):
            os.mkdir(self.model_dir)

    def train(self, train_data):
        self.anom_model.fit(train_data)

    def predict(self, test_data):
        return self.anom_model.predict(test_data)

    def train_predict(self, train_data):
        return self.anom_model.fit_predict(train_data)

    def save(self, project_name, col_name, metric_name, arguments=None):
        file_name = create_file_name(
            self.model_dir, project_name, col_name, metric_name, arguments
        )
        # todo: should be logged instead of printed
        # print(f"Writing anomaly model to {file_name}")
        joblib.dump(self.anom_model, file_name)


class LoadedModel(object):
    def __init__(self, config_loc=default_config_path):
        self.model_dir = os.path.join(config_loc, "models")

    def load(self, project_name, col_name, metric_name, arguments=None):
        file_name = create_file_name(
            self.model_dir, project_name, col_name, metric_name, arguments
        )
        # print(f"Loading model from {file_name}")
        self.anom_model = joblib.load(file_name)

    def predict(self, test_data):
        return self.anom_model.predict(test_data)

    def train_predict(self, train_data):
        return self.anom_model.fit_predict(train_data)


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
                try:
                    mod = AnomalyModel(config_loc=self.config_dir)
                    mod.train(data.value.values.reshape((-1, 1)))
                    mod.save(
                        self.project.project_name,
                        data.column_name.values[0],
                        data.metric.values[0],
                        data.arguments.values[0],
                    )
                    preds = mod.predict(data.value.values.reshape((-1, 1)))
                    outlier_rows = data[preds == -1]
                    if outlier_rows.shape[0] > 0:
                        all_rows.append(outlier_rows)
                except:
                    warnings.warn(f"Unable to create anomaly model for {metric_name}")

        columns = ["column_name", "date", "metric", "arguments", "value", "batch_name"]
        try:
            data = pd.concat(all_rows).sort_values("date", ascending=False)
            data = data[columns]
            data.value = data.value.astype(str)
        except:
            data = pd.DataFrame([], columns=columns)
        return data

    def create_anom_cat_table(self):
        df = self.project.get_project_table()
        df = df[df["metric"].isin(["value_counts"])]
        df["metric_name"] = (
            df.column_name
            + "_"
            + df.metric.astype(str)
            + "_"
            + np.where(df.arguments.isnull(), "", df.arguments)
        )
        all_rows = []
        for metric_name, data in df.groupby("metric_name"):
            try:
                data_values = [
                    (pd.Series(c) / pd.Series(c).sum()).to_dict() for c in data["value"]
                ]
                unique_vals = reduce(
                    lambda x, y: x.union(y), [set(i.keys()) for i in data_values]
                )
                potential_lines = []
                for cat in unique_vals:
                    values = pd.Series([i.get(cat, 0) for i in data_values])
                    running_means = values.rolling(window=5).mean()
                    differences = values - running_means
                    sum_abs = np.abs(differences).sum()
                    potential_lines.append((cat, differences, sum_abs))
                potential_lines = sorted(
                    potential_lines, key=lambda v: v[2], reverse=True
                )
                all_lines = pd.DataFrame({i[0]: i[1] for i in potential_lines})
                mod = AnomalyModel()
                outliers = mod.train_predict(all_lines.values[4:])
                outliers = np.concatenate([np.array([1, 1, 1, 1]), outliers])
                outlier_rows = data[outliers == -1]
                if outlier_rows.shape[0] > 0:
                    all_rows.append(outlier_rows)
            except ValueError:
                pass

        columns = ["column_name", "date", "metric", "arguments", "value", "batch_name"]
        try:
            data = pd.concat(all_rows).sort_values("date", ascending=False)
            data = data[columns]
            data.value = data.value.astype(str)
        except:
            data = pd.DataFrame([], columns=columns)
        return data


def anomaly_data_project(project_name, db_url, config_dir):
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


def anomaly_data_all_projects(project_names, db_url, config_dir):
    data = []
    for project in project_names:
        adata = anomaly_data_project(project, db_url, config_dir)
        adata["project"] = project
        adata = adata[["project"] + [col for col in adata.columns if col != "project"]]
        data.append(adata)
    if len(project_names) == 0:
        data = pd.DataFrame(
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
    else:
        data = pd.concat(data)
    data = data.sort_values("date")
    return data
