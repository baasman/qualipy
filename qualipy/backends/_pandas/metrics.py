import pandas as pd
import numpy as np
from scipy import stats

from qualipy.util import get_column
from qualipy.column import function


#numeric

@function(anomaly=False)
def mean(data, column):
    return data[column].mean()


def _get_std(data, column):
    return data[column].std()


def _get_min(data, column):
    return data[column].min()


def _get_quantile(data, column, quantile=.5):
    return data[column].quantile(quantile)


def _get_number_of_duplicates(data, column):
    return data.shape[0] - data.drop_duplicates().shape[0]

@function(anomaly=False)
def percentage_missing(data, column):
    return get_column(data, column).isnull().sum() / data.shape[0]

def _get_nunique(data, column):
    return get_column(data, column).nunique()


def _get_top(data, column):
    return data[column].describe()['top']


def _get_freq(data, column):
    return data[column].describe()['freq']

@function(anomaly=False)
def is_unique(data, column):
    if column == 'index':
        return data.index.unique().shape[0] == data.shape[0]
    return data[column].unique().shape[0] == data.shape[0]

def _get_correlation(data, column, column_two):
    return data[column].corr(data[column_two])

def _get_number_of_outliers(data, column, std_away):
    data = data[data[column].notnull()]
    return data[np.abs(stats.zscore(data[column])) > std_away].shape[0]


# non numeric

@function(anomaly=False)
def value_counts(data, column):
    return data[data[column] != 'nan'][column].value_counts().sort_values(ascending=False).head(10).to_dict()

def heatmap(data, column, column_two=None, include_nan=True):
    if include_nan:
        data = data[(data[column] != 'nan') & (data[column_two] != 'nan')]
    cross = pd.crosstab(data[column], data[column_two])
    cross_data = {
        'z': cross.values.tolist(),
        'y': cross.index.values.tolist(),
        'x': cross.columns.tolist()
    }
    return cross_data

def correlation(data, column, other_columns):
    cols_to_use = [column]
    cols_to_use.extend(other_columns)
    corrs = data[cols_to_use]
    corrs_data = {
        'z': corrs.values.tolist(),
        'y': corrs.index.values.tolist(),
        'x': corrs.columns.tolist()
    }
    return corrs_data
