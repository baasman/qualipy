from qualipy.project import Project

from sklearn.ensemble import IsolationForest
import joblib
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

try:
    from fbprophet import Prophet
except ImportError:
    print("fbprophet not installed. Can not be used for anomaly training")
from tqdm import tqdm

import os
import json
import warnings
from functools import reduce


default_config_path = os.path.join(os.path.expanduser("~"), ".qualipy")

anomaly_columns = [
    "column_name",
    "date",
    "metric",
    "arguments",
    "return_format",
    "value",
    "batch_name",
    "insert_time",
]


def create_file_name(model_dir, project_name, col_name, metric_name, arguments):
    file_name = os.path.join(
        model_dir, f"{project_name}_{col_name}_{metric_name}_{arguments}.mod"
    )
    return file_name


class ProphetModel(object):
    def __init__(self, kwargs):
        self.model = Prophet(**kwargs)

    def fit(self, train_data):
        train_data = train_data[["date", "value"]].rename(
            columns={"date": "ds", "value": "y"}
        )
        self.model.fit(train_data)

    def predict(self, test_data, check_for_std=False, multivariate=False):
        test_data = test_data[["date", "value"]].rename(
            columns={"date": "ds", "value": "y"}
        )
        predicted = self.model.predict(test_data)
        predicted = predicted.set_index(test_data.index)
        predicted["y"] = test_data["y"]

        predicted.loc[predicted.y > predicted.yhat, "importance"] = (
            predicted.y - predicted.yhat_upper
        ) / predicted.y
        predicted.loc[predicted.y < predicted.yhat, "importance"] = (
            predicted.yhat_lower - predicted.y
        ) / predicted.y
        predicted["outlier"] = np.where(
            (
                (predicted.y > predicted.yhat_upper)
                | (predicted.y < predicted.yhat_lower)
            )
            & (predicted.importance > 0.1),
            -1,
            1,
        )
        return predicted.outlier

    def train_predict(self, train_data):
        train_data = train_data[["date", "value"]].rename(
            columns={"date": "ds", "value": "y"}
        )
        predicted = self.model.fit(train_data).predict(train_data)
        predicted = predicted.set_index(train_data.index)
        predicted["y"] = train_data["y"]
        predicted["outlier"] = np.where(
            (predicted.y > predicted.yhat_upper) | (predicted.y < predicted.yhat_lower),
            -1,
            1,
        )
        return predicted.outlier


class IsolationForestModel(object):
    def __init__(self, kwargs):
        defaults = {"contamination": 0.05}
        kwargs = {**kwargs, **defaults}
        self.model = IsolationForest(**kwargs)

    def fit(self, train_data):
        self.model.fit(train_data.value.values.reshape((-1, 1)))

    def predict(self, test_data, check_for_std=False, multivariate=False):
        if multivariate:
            preds = self.model.predict(test_data)
        else:
            preds = self.model.predict(test_data.value.values.reshape((-1, 1)))

        if check_for_std and not multivariate:
            std = test_data.value.std()
            mean = test_data.value.mean()
            std_outliers = (test_data.value < mean - (3 * std)) | (
                test_data.value > mean + (3 * std)
            )
            preds = [
                -1 if mod_val == -1 and std else 1
                for mod_val, std in zip(preds, std_outliers)
            ]
        return np.array(preds)

    def train_predict(self, train_data, **kwargs):
        if isinstance(train_data, pd.DataFrame):
            self.model.fit(train_data)
            return self.predict(train_data, **kwargs)
        else:
            self.model.fit(train_data.value.values.reshape((-1, 1)))
            return self.predict(train_data.value.values.reshape((-1, 1)))


class AnomalyModel(object):

    mods = {"IsolationForest": IsolationForestModel, "prophet": ProphetModel}

    def __init__(self, config_loc, model=None, arguments=None):
        with open(os.path.join(config_loc, "config.json"), "r") as c:
            config = json.load(c)

        if model is None and arguments is None:
            model = config.get("ANOMALY_MODEL", "IsolationForest")
            arguments = config.get("ANOMALY_ARGS", {})
        self.anom_model = self.mods[model](arguments)
        self.model_dir = os.path.join(config_loc, "models")
        if not os.path.isdir(self.model_dir):
            os.mkdir(self.model_dir)

    def train(self, train_data):
        self.anom_model.fit(train_data)

    def predict(self, test_data, check_for_std=False, multivariate=False):
        return self.anom_model.predict(
            test_data, check_for_std=check_for_std, multivariate=multivariate
        )

    def train_predict(self, train_data, **kwargs):
        return self.anom_model.train_predict(train_data, **kwargs)

    def save(self, project_name, col_name, metric_name, arguments=None):
        file_name = create_file_name(
            self.model_dir, project_name, col_name, metric_name, arguments
        )
        # todo: should be logged instead of printed
        # print(f"Writing anomaly model to {file_name}")
        joblib.dump(self.anom_model, file_name)


class LoadedModel(object):
    def __init__(self, config_loc):
        self.model_dir = os.path.join(config_loc, "models")

    def load(self, project_name, col_name, metric_name, arguments=None):
        file_name = create_file_name(
            self.model_dir, project_name, col_name, metric_name, arguments
        )
        # print(f"Loading model from {file_name}")
        self.anom_model = joblib.load(file_name)

    def predict(self, test_data, check_for_std=False, multivariate=False):
        return self.anom_model.predict(
            test_data, check_for_std=check_for_std, multivariate=multivariate
        )

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
        self.project = Project(project_name, config_dir=config_dir)

    def _num_train_and_save(self, data, all_rows, metric_name):
        try:
            mod = AnomalyModel(config_loc=self.config_dir)
            # mod.train(data.value.values.reshape((-1, 1)))
            mod.train(data)
            mod.save(
                self.project.project_name,
                data.column_name.values[0],
                data.metric.values[0],
                data.arguments.values[0],
            )
            # preds = mod.predict(data.value.values.reshape((-1, 1)))
            preds = mod.predict(data, multivariate=False, check_for_std=True)
            outlier_rows = data[preds == -1]
            if outlier_rows.shape[0] > 0:
                all_rows.append(outlier_rows)
        except:
            warnings.warn(f"Unable to create anomaly model for {metric_name}")
        return all_rows

    def _num_from_loaded_model(self, data, all_rows):
        mod = LoadedModel(config_loc=self.config_dir)
        mod.load(
            self.project.project_name,
            data.column_name.values[0],
            data.metric.values[0],
            data.arguments.values[0],
        )
        preds = mod.predict(data, check_for_std=True)
        outlier_rows = data[preds == -1]
        if outlier_rows.shape[0] > 0:
            all_rows.append(outlier_rows)
        return all_rows

    def create_anom_num_table(self, retrain=False):
        df = self.project.get_project_table()
        df = df[
            (df["type"] == "numerical") | (df["column_name"].isin(["rows", "columns"]))
        ]
        df = (
            df.groupby("batch_name", as_index=False)
            .apply(lambda g: g[g.insert_time == g.insert_time.max()])
            .reset_index(drop=True)
        )
        df.value = df.value.astype(float)
        df.column_name = df.column_name + "_" + df.run_name
        df["metric_name"] = (
            df.column_name
            + "_"
            + df.metric.astype(str)
            + "_"
            + np.where(df.arguments.isnull(), "", df.arguments)
        )
        all_rows = []
        for metric_name, data in tqdm(df.groupby("metric_name")):

            if not retrain:
                try:
                    all_rows = self._num_from_loaded_model(data, all_rows)
                except ValueError:
                    warnings.warn(f"Unable to load anomaly model for {metric_name}")
                except FileNotFoundError:
                    all_rows = self._num_train_and_save(data, all_rows, metric_name)
            else:
                all_rows = self._num_train_and_save(data, all_rows, metric_name)

        try:
            data = pd.concat(all_rows).sort_values("date", ascending=False)
            data = data[anomaly_columns]
            data.value = data.value.astype(str)
        except:
            data = pd.DataFrame([], columns=anomaly_columns)
        return data

    def create_anom_cat_table(self, retrain=False):
        df = self.project.get_project_table()
        df = (
            df.groupby("batch_name", as_index=False)
            .apply(lambda g: g[g.insert_time == g.insert_time.max()])
            .reset_index(drop=True)
        )
        df = df[df["type"] == "categorical"]
        df.column_name = df.column_name + "_" + df.run_name
        df["metric_name"] = (
            df.column_name
            + "_"
            + df.metric.astype(str)
            + "_"
            + np.where(df.arguments.isnull(), "", df.arguments)
        )
        all_rows = []
        for metric_name, data in tqdm(df.groupby("metric_name")):
            try:
                data_values = [
                    (pd.Series(c) / pd.Series(c).sum()).to_dict() for c in data["value"]
                ]
                unique_vals = reduce(
                    lambda x, y: x.union(y), [set(i.keys()) for i in data_values]
                )
                non_diff_lines = []
                potential_lines = []
                for cat in unique_vals:
                    values = pd.Series([i.get(cat, 0) for i in data_values])
                    running_means = values.rolling(window=5).mean()
                    differences = values - running_means
                    sum_abs = np.abs(differences).sum()
                    potential_lines.append((cat, differences, sum_abs))
                    non_diff_lines.append((cat, values))
                potential_lines = sorted(
                    potential_lines, key=lambda v: v[2], reverse=True
                )
                all_lines = pd.DataFrame({i[0]: i[1] for i in potential_lines})
                all_non_diff_lines = pd.DataFrame({i[0]: i[1] for i in non_diff_lines})

                for col in all_non_diff_lines.columns:
                    mean = all_non_diff_lines[col].mean()
                    std = all_non_diff_lines[col].std()
                    all_non_diff_lines[f"{col}_below"] = np.where(
                        all_non_diff_lines[col] < (mean - (3 * std)), 1, 0
                    )
                    all_non_diff_lines[f"{col}_above"] = np.where(
                        all_non_diff_lines[col] > (mean + (3 * std)), 1, 0
                    )
                std_sums = all_non_diff_lines[
                    [
                        col
                        for col in all_non_diff_lines.columns
                        if "_below" in str(col) or "_above" in str(col)
                    ]
                ].sum(axis=1)

                mod = AnomalyModel(
                    config_loc=self.config_dir,
                    model="IsolationForest",
                    arguments={"contamination": 0.01, "n_estimators": 50,},
                )
                outliers = mod.train_predict(
                    all_non_diff_lines, check_for_std=False, multivariate=True
                )
                outlier_rows = data[(outliers == -1) & (std_sums.values > 0)]
                if outlier_rows.shape[0] > 0:
                    all_rows.append(outlier_rows)
            except ValueError:
                pass

        try:
            data = pd.concat(all_rows).sort_values("date", ascending=False)
            data = data[anomaly_columns]
            data.value = data.value.astype(str)
        except:
            data = pd.DataFrame([], columns=anomaly_columns)
        return data


def anomaly_data_project(project_name, db_url, config_dir, retrain):
    engine = create_engine(db_url)
    generator = GenerateAnomalies(project_name, engine, config_dir)
    try:
        cat_anomalies = generator.create_anom_cat_table(retrain)
        num_anomalies = generator.create_anom_num_table(retrain)
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


# TODO: why is db url a input when its already part of the config dir
def anomaly_data_all_projects(project_names, db_url, config_dir, retrain=False):
    data = []
    if isinstance(project_names, str):
        project_names = [project_names]
    for project in project_names:
        adata = anomaly_data_project(project, db_url, config_dir, retrain)
        adata["project"] = project
        adata = adata[["project"] + [col for col in adata.columns if col != "project"]]
        data.append(adata)
    if len(project_names) == 0:
        data = pd.DataFrame([], columns=anomaly_columns)
    else:
        data = pd.concat(data)
    data = data.sort_values("date")
    return data


def _run_anomaly(backend, project_name, config_dir, retrain):
    with open(os.path.join(config_dir, "config.json"), "r") as file:
        loaded_config = json.load(file)
    qualipy_db = loaded_config.get(
        "QUALIPY_DB", f"sqlite:///{os.path.join(config_dir, 'qualipy.db')}"
    )
    anom_data = anomaly_data_all_projects(
        project_name, qualipy_db, config_dir, retrain=retrain
    )
    engine = create_engine(qualipy_db)
    db_schema = loaded_config.get("SCHEMA")
    with engine.connect() as conn:
        backend.write_anomaly(
            conn, anom_data, project_name, clear=retrain, schema=db_schema
        )

