import warnings
from functools import reduce

import numpy as np
import pandas as pd
from tqdm import tqdm

from qualipy.project import Project
from qualipy.util import set_value_type, set_metric_id
from qualipy.anomaly.models import AnomalyModel, LoadedModel


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


class GenerateAnomalies(object):
    def __init__(self, project_name, engine, config_dir):
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
            preds = mod.predict(data)
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
        preds = mod.predict(data)
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
        for metric_id, data in tqdm(df.groupby("metric_id")):
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
                    metric_name=metric_id,
                    model="IsolationForest",
                    arguments={
                        "contamination": 0.01,
                        "n_estimators": 50,
                        "multivariate": True,
                        "check_for_std": True,
                    },
                )
                # make sure this is still doing multivariate
                outliers = mod.train_predict(all_non_diff_lines)
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
