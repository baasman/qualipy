import pandas as pd
from qualipy import DataSet
import random


def mean_plus_n(data, column, n):
    return data[column].mean() + n


iris = {
    'data_name': 'iris',
    'columns': {
        'petal_length': {
            'type': 'float',
            'metrics': [
                'mean',
                'std',
                {'function': 'mean_plus_n', 'parameters': {'n': 1}}
            ]
        },
        'petal_width': {
            'type': 'float',
            'metrics': [
                'mean',
                {'function': 'quantile', 'parameters': {'quantile': .5}},
                {'function': 'quantile', 'parameters': {'quantile': .25}},
            ]
        },
        'variety': {
            'type': 'string',
            'metrics': ['nunique']
        }
    },
    'custom_functions': {
        'mean_plus_n': mean_plus_n
    }
}

ts = pd.date_range(start='1/1/2018', end='1/30/2018')
for idx, time in enumerate(ts):
    data = pd.read_csv('https://gist.githubusercontent.com/netj/8836201/raw/6f9306ad21398ea43cba4f7d537619d0e07d5ae3/iris.csv')
    cols = [i.replace('.', '_') for i in data.columns]
    data.columns = cols
    data['petal_length'] += random.randint(0, 5)
    data['petal_width'] += random.randint(0, 5)
    if idx == 0:
        ds = DataSet(iris, reset=True, time_of_run=time)
    else:
        ds = DataSet(iris, reset=False, time_of_run=time)
    ds.set_dataset(data)
    ds.run()

