import pandas as pd
import numpy as np
from scipy import stats

from qualipy.util import get_column
from qualipy.column import function


# numeric


@function(return_format=float)
def mean(data, column):
    return data[column].mean()


@function(return_format=float)
def std(data, column):
    return data[column].std()


@function()
def min(data, column):
    return data[column].min()


@function(allowed_arguments=["quantile"], return_format=float)
def quantile(data, column, quantile=0.5):
    return data[column].quantile(quantile)


@function(return_format=int)
def number_of_duplicates(data, column):
    return data.shape[0] - data.drop_duplicates().shape[0]


@function(return_format=float)
def percentage_missing(data, column):
    missing_data = data[(data[column].isnull()) | (data[column] == "")]
    try:
        return missing_data.shape[0] / data.shape[0]
    except ZeroDivisionError:
        return 0


@function(return_format=int)
def nunique(data, column):
    return get_column(data, column).nunique()


@function()
def get_top(data, column):
    return data[column].describe()["top"]


@function()
def freq(data, column):
    return data[column].describe()["freq"]


@function(return_format=bool)
def is_unique(data, column):
    if column == "index":
        return data.index.unique().shape[0] == data.shape[0]
    return data[column].unique().shape[0] == data.shape[0]


@function(allowed_arguments=["column_two"], return_format=float)
def correlation_two_columns(data, column, column_two):
    return data[column].corr(data[column_two])


@function(allowed_arguments=["std_away"], return_format=int)
def number_of_outliers(data, column, std_away):
    data = data[data[column].notnull()]
    return data[np.abs(stats.zscore(data[column])) > std_away].shape[0]


# non numeric


@function(return_format=dict)
def value_counts(data, column):
    return (
        data[data[column] != "nan"][column]
        .value_counts()
        .sort_values(ascending=False)
        .head(10)
        .to_dict()
    )


@function(allowed_arguments=["include_nan", "column_two"], return_format=dict)
def heatmap(data, column, column_two=None, include_nan=True):
    if include_nan:
        data = data[(data[column] != "nan") & (data[column_two] != "nan")]
    cross = pd.crosstab(data[column], data[column_two])
    cross_data = {
        "z": cross.values.tolist(),
        "y": cross.index.values.tolist(),
        "x": cross.columns.tolist(),
    }
    return cross_data


@function(return_format=dict, allowed_arguments=["column_two"])
def correlation(data, column, column_two):
    corrs = pd.DataFrame(
        {column: data[column].values, "other_col": data[column_two].values}
    )
    corrs_data = {
        "z": corrs.values.tolist(),
        "y": corrs.index.values.tolist(),
        "x": corrs.columns.tolist(),
    }
    return corrs_data


@function(return_format=dict, allowed_arguments=["time_freq"])
def events_per_time_period(data, column, time_freq="1D"):
    d = data.copy()
    counts = d.groupby(pd.Grouper(key=column, freq=time_freq)).apply(
        lambda g: g.shape[0]
    )
    counts = {str(k): v for k, v in counts.items()}
    return counts
