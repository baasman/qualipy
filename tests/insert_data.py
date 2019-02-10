import pandas as pd
from qualipy import DataSet
import random


def mean_plus_n(column, n):
    return column.mean() + n


iris = {
    'data_name': 'iris',
    'columns': {
        'petal.length': {
            'type': 'float',
            'metrics': [
                'mean',
                'std',
                {'function': 'mean_plus_n', 'parameters': {'n': 1}}
            ]
        },
        'petal.width': {
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

for _ in range(20):
    data = pd.read_csv('https://gist.githubusercontent.com/netj/8836201/raw/6f9306ad21398ea43cba4f7d537619d0e07d5ae3/iris.csv')
    data['petal.length'] += random.randint(0, 5)
    data['petal.width'] += random.randint(0, 5)
    if _ == 0:
        ds = DataSet(iris, reset=True)
    else:
        ds = DataSet(iris, reset=False)
    ds.read_pandas(data)
    ds.run()

