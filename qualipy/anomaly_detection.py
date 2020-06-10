from qualipy.project import Project
from qualipy.util import set_value_type, set_metric_id

from sklearn.ensemble import IsolationForest
from scipy.stats import zscore
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
    "severity",
    "batch_name",
    "insert_time",
]


def create_file_name(model_dir, metric_id):
    file_name = os.path.join(model_dir, f"{metric_id}.mod")
    return file_name


class ProphetModel(object):
    def __init__(self, metric_name, kwargs):
        self.check_for_std = kwargs.pop("check_for_std", False)
        self.importance_level = kwargs.pop("importance_level", 0)
        self.distance_from_bound = kwargs.pop("distance_from_bound", 0)
        _model_specific_conf = kwargs.pop("metric_specific_conf", {})
        self.model_specific_conf = _model_specific_conf.get(metric_name, {})

        self.importance_level = self.model_specific_conf.get(
            "importance_level", self.importance_level
        )
        self.distance_from_bound = self.model_specific_conf.get(
            "distance_from_bound", self.distance_from_bound
        )

        self.model = Prophet(**kwargs)

    def fit(self, train_data):
        train_data = train_data[["date", "value"]].rename(
            columns={"date": "ds", "value": "y"}
        )
        train_data.ds = train_data.ds.dt.tz_localize(None)
        self.model.fit(train_data)

    def predict(self, test_data, check_for_std=False, multivariate=False):
        test_data = test_data[["date", "value"]].rename(
            columns={"date": "ds", "value": "y"}
        )
        test_data.ds = test_data.ds.dt.tz_localize(None)
        predicted = self.model.predict(test_data)
        predicted = predicted.set_index(test_data.index)
        predicted["y"] = test_data["y"]

        predicted["outlier"] = 1
        predicted["outlier"] = np.where(predicted.y < predicted.yhat_lower, -1, 1)
        predicted["outlier"] = np.where(predicted.y > predicted.yhat_upper, -1, 1)

        predicted["importance"] = np.NaN
        predicted.loc[predicted.outlier == 1, "importance"] = (
            predicted.y - predicted.yhat_upper
        ) / (predicted.yhat_upper - predicted.yhat_lower)
        predicted.loc[predicted.outlier == -1, "importance"] = (
            predicted.yhat_lower - predicted.y
        ) / (predicted.yhat_upper - predicted.yhat_lower)
        predicted.importance = predicted.importance.abs()
        predicted["score"] = (predicted["y"] - predicted["yhat_upper"]) * (
            predicted["y"] >= predicted["yhat"]
        ) + (predicted["yhat_lower"] - predicted["y"]) * (
            predicted["y"] < predicted["yhat"]
        )
        predicted["standardized_score"] = (
            (predicted.score - predicted.score.mean()) / predicted.score.std()
        ).abs()
        if self.check_for_std:
            std = test_data.y.std(ddof=0)
            mean = test_data.y.mean()
            zscores = (test_data.y - mean) / std
            std_outliers = (zscores < -3) | (zscores > 3)
            predicted["zscore_outlier"] = std_outliers
            predicted["zscore"] = zscores
            predicted["outlier"] = np.where(
                (predicted.outlier == -1)
                & (predicted.standardized_score > self.distance_from_bound)
                & (predicted.importance > self.importance_level)
                & ((predicted.y < 3) | (predicted.y > 3)),
                -1,
                1,
            )
        else:
            predicted["outlier"] = np.where(
                (predicted.outlier == -1)
                & (predicted.standardized_score > self.distance_from_bound)
                & (predicted.importance > self.importance_level),
                -1,
                1,
            )
            if predicted[predicted.outlier == -1].shape[0] > 0:
                print(predicted.head())
        return predicted.outlier.values, predicted.importance.values

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
    def __init__(self, metric_name, kwargs):
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


class STDCheck(object):
    def __init__(self, metric_name, kwargs):
        defaults = {"std": 4}
        kwargs = {**defaults, **kwargs}
        for k, v in kwargs.items():
            setattr(self, k, v)

    def fit(self, train_data):
        self.model = None

    def predict(self, test_data, check_for_std=False, multivariate=False):
        metric_name = test_data.metric.iloc[0]
        if multivariate:
            raise NotImplementedError
        else:
            std = test_data.value.std()
            mean = test_data.value.mean()

            # its useless to check for result set if count is really low
            # fix hardcoding of metric name here
            if (metric_name in ["count", "number_of_rows_per_patient"]) and std < 5:
                std_outliers = np.repeat(False, test_data.shape[0])
                zscores = np.repeat(0, test_data.shape[0])
            else:
                low = mean - (self.std * std)
                high = mean + (self.std * std)
                zscores = zscore(test_data.value)
                std_outliers = (zscores < -self.std) | (zscores > self.std)
            preds = [-1 if std == True else 1 for std in std_outliers]

        return np.array(preds), zscores

    def train_predict(self, train_data, **kwargs):
        if isinstance(train_data, pd.DataFrame):
            self.model.fit(train_data)
            return self.predict(train_data, **kwargs)
        else:
            self.model.fit(train_data.value.values.reshape((-1, 1)))
            return self.predict(train_data.value.values.reshape((-1, 1)))


class AnomalyModel(object):

    mods = {
        "IsolationForest": IsolationForestModel,
        "prophet": ProphetModel,
        "std": STDCheck,
    }

    def __init__(
        self, config_loc, metric_name, project_name=None, model=None, arguments=None,
    ):
        with open(os.path.join(config_loc, "config.json"), "r") as c:
            config = json.load(c)

        self.metric_name = metric_name
        if model is None and arguments is None:
            model = config[project_name].get("ANOMALY_MODEL", "std")
            arguments = config[project_name].get("ANOMALY_ARGS", {})
        self.anom_model = self.mods[model](self.metric_name, arguments)
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

    def save(self):
        file_name = create_file_name(self.model_dir, self.metric_name)
        joblib.dump(self.anom_model, file_name)


class LoadedModel(object):
    def __init__(self, config_loc):
        self.model_dir = os.path.join(config_loc, "models")

    def load(self, metric_id):
        file_name = create_file_name(self.model_dir, metric_id)
        self.anom_model = joblib.load(file_name)

    def predict(self, test_data, check_for_std=False, multivariate=False):
        return self.anom_model.predict(
            test_data, check_for_std=check_for_std, multivariate=multivariate
        )

    def train_predict(self, train_data):
        return self.anom_model.fit_predict(train_data)


class GenerateAnomalies(object):
    def __init__(self, project_name, engine, config_dir=default_config_path):
        self.config_dir = config_dir
        self.project_name = project_name
        self.project = Project(project_name, config_dir=config_dir)
        df = self.project.get_project_table()
        df["floored_datetime"] = df.date.dt.floor("T")
        df = (
            df.groupby("floored_datetime", as_index=False)
            .apply(lambda g: g[g.insert_time == g.insert_time.max()])
            .reset_index(drop=True)
        )
        df = df.drop("floored_datetime", axis=1)
        df.column_name = df.column_name + "_" + df.run_name
        df["metric_name"] = (
            df.column_name
            + "_"
            + df.metric.astype(str)
            + "_"
            + np.where(df.arguments.isnull(), "", df.arguments)
        )
        df = set_metric_id(df)
        df = df.sort_values("date")
        df = df[df.column_name.str.contains('vaso')]
        self.df = df

    def _num_train_and_save(self, data, all_rows, metric_name):
        try:
            metric_id = data.metric_id.iloc[0]
            mod = AnomalyModel(
                config_loc=self.config_dir,
                metric_name=metric_id,
                project_name=self.project_name,
            )
            mod.train(data)
            mod.save()
            preds = mod.predict(data, multivariate=False, check_for_std=True)
            if isinstance(preds, tuple):
                severity = preds[1]
                preds = preds[0]
                outlier_rows = data[preds == -1].copy()
                outlier_rows["severity"] = severity[preds == -1]
            else:
                outlier_rows = data[preds == -1]
                outlier_rows["severity"] = np.NaN
            if outlier_rows.shape[0] > 0:
                all_rows.append(outlier_rows)
        except:
            warnings.warn(f"Unable to create anomaly model for {metric_name}")
        return all_rows

    def _num_from_loaded_model(self, data, all_rows):
        mod = LoadedModel(config_loc=self.config_dir)
        mod.load(data.metric_id.iloc[0])
        preds = mod.predict(data, check_for_std=True)
        if isinstance(preds, tuple):
            reasons = preds[1]
            preds = preds[0]
        outlier_rows = data[preds == -1]
        if outlier_rows.shape[0] > 0:
            all_rows.append(outlier_rows)
        return all_rows

    def create_anom_num_table(self, retrain=False):
        df = self.df.copy()
        df = df[
            (df["type"] == "numerical")
            | (df["column_name"].isin(["rows", "columns"]))
            | (df["metric"] == "perc_missing")
        ]
        df.value = df.value.astype(float)
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
        df = self.df
        df = df[df["type"] == "categorical"]
        all_rows = []
        for metric_name, data in tqdm(df.groupby("metric_name")):
            data = set_value_type(data.copy())
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
                diffs_df = pd.DataFrame({i[0]: i[1] for i in potential_lines})
                diffs_df["sum_of_changes"] = diffs_df.abs().sum(axis=1)
                all_non_diff_lines = pd.DataFrame({i[0]: i[1] for i in non_diff_lines})

                for col in all_non_diff_lines.columns:
                    mean = all_non_diff_lines[col].mean()
                    std = all_non_diff_lines[col].std()
                    if std > 0.05:
                        all_non_diff_lines[f"{col}_below"] = np.where(
                            all_non_diff_lines[col] < (mean - (4 * std)), 1, 0
                        )
                        all_non_diff_lines[f"{col}_above"] = np.where(
                            all_non_diff_lines[col] > (mean + (4 * std)), 1, 0
                        )
                    else:
                        all_non_diff_lines[f"{col}_below"] = 0
                        all_non_diff_lines[f"{col}_above"] = 0

                std_sums = all_non_diff_lines[
                    [
                        col
                        for col in all_non_diff_lines.columns
                        if "_below" in str(col) or "_above" in str(col)
                    ]
                ].sum(axis=1)

                mod = AnomalyModel(
                    config_loc=self.config_dir,
                    metric_name=data.metric_id.iloc[0],
                    model="IsolationForest",
                    arguments={"contamination": 0.01, "n_estimators": 50,},
                )
                outliers = mod.train_predict(
                    all_non_diff_lines, check_for_std=False, multivariate=True
                )
                all_non_diff_lines["iso_outlier"] = outliers
                data["severity"] = diffs_df.sum_of_changes.values
                sample_size = data.value.apply(lambda v: sum(v.values()))
                outlier_rows = data[
                    (outliers == -1) & (std_sums.values > 0) & (sample_size > 10)
                ]
                if outlier_rows.shape[0] > 0:
                    all_rows.append(outlier_rows)
            except ValueError:
                pass

        try:
            data = pd.concat(all_rows).sort_values("date", ascending=False)
            # data["severity"] = np.NaN
            data = data[anomaly_columns]
            data.value = data.value.astype(str)
        except:
            data = pd.DataFrame([], columns=anomaly_columns)
        return data

    def create_error_check_table(self):
        # obv only need to do this once
        df = self.df
        df = df[df["type"] == "boolean"]
        if df.shape[0] > 0:
            df = set_value_type(df)
            df = df[~df.value]
            df["severity"] = np.NaN
            df = df[anomaly_columns]
        else:
            df = pd.DataFrame([], columns=anomaly_columns)
        return df


def anomaly_data_project(project_name, db_url, config_dir, retrain):
    engine = create_engine(db_url)
    generator = GenerateAnomalies(project_name, engine, config_dir)
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


def _run_anomaly(backend, project_name, config_dir, retrain):
    with open(os.path.join(config_dir, "config.json"), "r") as file:
        loaded_config = json.load(file)
    qualipy_db = loaded_config["QUALIPY_DB"]
    anom_data = anomaly_data_project(
        project_name=project_name,
        db_url=qualipy_db,
        config_dir=config_dir,
        retrain=retrain,
    )
    engine = create_engine(qualipy_db)
    db_schema = loaded_config.get("SCHEMA")
    with engine.connect() as conn:
        write_anomaly(conn, anom_data, project_name, clear=retrain, schema=db_schema)
