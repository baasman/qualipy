import json
import os
import itertools

import pandas as pd
import numpy as np
from scipy import stats
from matplotlib.pyplot import hist


def cramers_corrected_stat(confusion_matrix, correction: bool = True) -> float:
    """Calculate the Cramer's V corrected stat for two variables.

    Args:
        confusion_matrix: Crosstab between two variables.
        correction: Should the correction be applied?

    Returns:
        The Cramer's V corrected stat for the two variables.
    """
    chi2 = stats.chi2_contingency(confusion_matrix, correction=correction)[0]
    n = confusion_matrix.sum().sum()
    phi2 = chi2 / n
    r, k = confusion_matrix.shape

    # Deal with NaNs later on
    with np.errstate(divide="ignore", invalid="ignore"):
        phi2corr = max(0.0, phi2 - ((k - 1.0) * (r - 1.0)) / (n - 1.0))
        rcorr = r - ((r - 1.0) ** 2.0) / (n - 1.0)
        kcorr = k - ((k - 1.0) ** 2.0) / (n - 1.0)
        corr = np.sqrt(phi2corr / min((kcorr - 1.0), (rcorr - 1.0)))
    return corr


def generate_hist(column_data, bins):
    histogram = hist(column_data, bins=30)
    histogram = [histogram[0].tolist(), histogram[1].tolist()]
    return histogram


class PandasBatchProfiler:
    def __init__(self, data, batch_name, run_name, columns, config_dir, project_name):
        self.data = data
        self.batch_name = batch_name
        self.columns = columns
        self.data = self.data[list(columns.keys())]
        self.config_dir = config_dir
        self.run_name = run_name
        self.data = self.data.fillna(np.NaN)
        self.project_name = project_name
        self._read_config()

    def _read_config(self):
        with open(os.path.join(self.config_dir, "config.json"), "rb") as f:
            config = json.load(f)
        profile_args = config[self.project_name].get("PROFILE_ARGS", {})
        self.facet_categorical_by = profile_args.get("facet_categorical_by")
        self.facet_numerical_by = profile_args.get("facet_numerical_by")

    def profile(self):
        head = self._head()
        dups = self._duplicated()
        num_info = self._num_info()
        cat_info = self._cat_info()
        num_corrs = self._get_num_correlation()
        cat_corrs = self._get_cat_correlation()
        missing_info = self._get_missing_info()
        # unable to install phik
        # mixed_corrs = self._get_mixed_correlation()
        self._serialize_to_json(
            head, dups, num_info, cat_info, num_corrs, cat_corrs, missing_info
        )

    def _head(self):
        return self.data.head().to_dict(orient="records")

    def _duplicated(self):
        duplicated_records = self.data.duplicated(keep="last")
        dups = self.data[duplicated_records].head(10)
        return {
            "head_of_dups": dups.to_dict(orient="records"),
            "number_of_duplicates": sum(duplicated_records),
            "percentage_of_duplicates": sum(duplicated_records)
            / duplicated_records.shape[0],
        }

    def _num_info(self):
        numeric_columns = [k for k, v in self.columns.items() if not v["is_category"]]
        num_data = self.data[numeric_columns]
        num_info = {}
        for column in num_data.columns:
            description = num_data[column].describe().to_dict()
            distinct = num_data[column].nunique()
            histogram = generate_hist(num_data[column], bins=30)
            if self.facet_numerical_by is not None:
                num_facets = {}
                for cat in self.data[self.facet_numerical_by].unique():
                    num_facets[str(cat)] = generate_hist(
                        self.data[self.data[self.facet_numerical_by] == cat][column],
                        bins=30,
                    )
            else:
                num_facets = {}

            num_info[column] = {
                "mean": description["mean"],
                "min": description["min"],
                "max": description["max"],
                "distinct": distinct,
                "missing": num_data[column].isna().sum() / num_data.shape[0],
                "kurtosis": stats.kurtosis(
                    num_data[num_data[column].notnull()][column]
                ),
                "skewness": stats.skew(num_data[num_data[column].notnull()][column]),
                "histogram": histogram,
                "num_facets": num_facets,
            }
        return num_info

    def _cat_info(self):
        category_columns = [k for k, v in self.columns.items() if v["is_category"]]
        cat_data = self.data[category_columns]
        cat_info = {}
        for column in cat_data.columns:
            distinct = cat_data[column].unique()
            try:
                distinct_perc = (
                    distinct.shape[0] / cat_data[cat_data[column].notnull()].shape[0]
                )
            except:
                distinct_perc = 0
            top_groups = (
                cat_data[cat_data[column].notnull()][column]
                .value_counts()
                .sort_values(ascending=False)
                .head(10)
            )
            other_groups = {
                "other_values": cat_data[
                    (cat_data[column].notnull())
                    & (~cat_data[column].isin(top_groups.index))
                ].shape[0]
            }
            if self.facet_categorical_by is not None:
                cat_facets = {}
                for cat in top_groups.index:
                    cat_facets[cat] = (
                        cat_data[
                            (cat_data[column].notnull()) & (cat_data[column] == cat)
                        ][self.facet_categorical_by]
                        .value_counts()
                        .sort_values(ascending=False)
                        .head(10)
                        .to_dict()
                    )
            else:
                cat_facets = {}

            top_groups_freq = (
                top_groups / cat_data[cat_data[column].notnull()].shape[0]
            ).to_dict()
            top_groups = top_groups.to_dict()
            if distinct.shape[0] > 10:
                top_groups = {**top_groups, **other_groups}
                top_groups_freq = {
                    **top_groups_freq,
                    **{
                        "other_values": other_groups["other_values"]
                        / cat_data[cat_data[column].notnull()].shape[0]
                    },
                }
            cat_info[column] = {
                "distinct": distinct.shape[0],
                "distinct (%)": distinct_perc,
                "missing": cat_data[column].isna().sum() / cat_data.shape[0],
                "top_groups": top_groups,
                "top_groups_freq": top_groups_freq,
                "cat_facets": cat_facets,
            }
        return cat_info

    def _get_num_correlation(self):
        numeric_columns = [k for k, v in self.columns.items() if not v["is_category"]]
        num_data = self.data[numeric_columns]
        corr = num_data.corr(method="spearman")
        corr = (
            corr.stack()
            .reset_index()
            .rename(
                columns={
                    "level_0": "Variable 1",
                    "level_1": "Variable 2",
                    0: "Correlation",
                }
            )
        )
        return corr.to_dict(orient="records")

    def _get_cat_correlation(self):
        category_columns = [
            k
            for k, v in self.columns.items()
            if v["is_category"]
            and self.data[k].nunique() > 1
            and self.data[k].nunique() < 50
        ]
        cat_data = self.data[category_columns]

        correlation_matrix = pd.DataFrame(
            np.ones((cat_data.shape[1], cat_data.shape[1])),
            index=cat_data.columns,
            columns=cat_data.columns,
        )
        categoricals = {
            column_name: cat_data[column_name] for column_name in cat_data.columns
        }
        for (name1, data1), (name2, data2) in itertools.combinations(
            categoricals.items(), 2
        ):
            confusion_matrix = pd.crosstab(data1, data2)
            correlation_matrix.loc[name2, name1] = correlation_matrix.loc[
                name1, name2
            ] = cramers_corrected_stat(confusion_matrix)

        correlation_matrix = (
            correlation_matrix.stack()
            .reset_index()
            .rename(
                columns={
                    "level_0": "Variable 1",
                    "level_1": "Variable 2",
                    0: "Correlation",
                }
            )
        )
        return correlation_matrix.to_dict(orient="records")

    def _get_mixed_correlation(self):
        cat_columns = [
            k
            for k in self.columns.keys()
            if self.data[k].nunique() > 1 and self.data[k].nunique() < 50
        ]
        numeric_columns = [k for k, v in self.columns.items() if not v["is_category"]]
        columns = cat_columns + numeric_columns
        data = self.data[columns]

        return

    def _get_missing_info(self):
        missing_counts = (self.data.isnull().sum() / self.data.shape[0]).to_dict()

        corr_data = self.data.iloc[
            :,
            [i for i, n in enumerate(np.var(self.data.isnull(), axis="rows")) if n > 0],
        ]
        if corr_data.shape[1] > 1:
            corr = corr_data.isnull().corr()
            corr = (
                corr.stack()
                .reset_index()
                .rename(
                    columns={
                        "level_0": "Variable 1",
                        "level_1": "Variable 2",
                        0: "Correlation",
                    }
                )
                .to_dict(orient="records")
            )
        else:
            corr = []
        return {
            "missing_counts": missing_counts,
            "missing_correlation": corr,
        }

    def _serialize_to_json(
        self, head, dups, num_info, cat_info, num_corr, cat_corr, missing
    ):
        data = {
            "head": head,
            "duplicates": dups,
            "numerical_info": num_info,
            "cat_info": cat_info,
            "num_corr": num_corr,
            "cat_corr": cat_corr,
            "missing": missing,
        }
        with open(
            os.path.join(
                self.config_dir,
                "profile_data",
                f"{str(self.batch_name)}-{str(self.run_name)}.json",
            ),
            "w",
        ) as f:
            json.dump(data, f)
