import pandas as pd
import numpy as np
from scipy import stats

import warnings

from qualipy.util import get_column
from qualipy.reflect.function import function


# numeric


@function(return_format=float, input_format=float)
def mean(data, column):
    return data[column].mean()


@function(return_format=float, input_format=float)
def count(data, column):
    return data.shape[0]


@function(return_format=float, input_format=float)
def std(data, column):
    return data[column].std()


@function(return_format=float, input_format=float)
def max(data, column):
    return data[column].max()


@function(return_format=float, input_format=float)
def min(data, column):
    return data[column].min()


@function(allowed_arguments=["quantile"], return_format=float, input_format=float)
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
        return 1


@function(
    return_format=int,
    display_name="Number of Unique Elements",
    description="This is a raw count of the total number of unique elements in the column",
)
def number_of_unique(data, column):
    return data[column].nunique()


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
    if data[column].nunique() > 100:
        warnings.warn(
            f"Too many unique columns for column {column}. Ignoring value counts"
        )
        return np.NaN
    return (
        data[data[column] != "nan"][column]
        .value_counts()
        .sort_values(ascending=False)
        .head(100)
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


@function(return_format=dict, allowed_arguments=["time_freq", "epoch_datetime"])
def events_per_time_period(data, column, epoch_datetime="max", time_freq="1D"):
    d = data.copy()
    d[column] = pd.to_datetime(d[column])
    d = d[d[column].notnull()]
    counts = d.groupby(pd.Grouper(key=column, freq=time_freq)).apply(
        lambda g: g.shape[0]
    )
    if epoch_datetime == "max":
        epoch_datetime = pd.to_datetime(d[column]).max()
    else:
        epoch_datetime = pd.to_datetime(epoch_datetime)
    if "D" in time_freq:
        fun = lambda k: abs(k - epoch_datetime).days
    elif "W" in time_freq:
        fun = lambda k: abs(k - epoch_datetime).days // 7
    elif "Y" in time_freq:
        fun = lambda k: abs(k - epoch_datetime).days // 365
    counts = {fun(k): v for k, v in counts.items()}
    return counts
