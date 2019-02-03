import dash_core_components as dcc
import numpy as np


def create_trend_line(data, var, metric):
    plot = dcc.Graph(
        id='num-data-graph-{}-{}'.format(var, metric),
        figure={
            'data': [
                {'y': data[(data['_name'] == var) &
                               (data['_metric'] == metric)]['value'],
                 'x': np.arange(data[(data['_name'] == var) &
                                         (data['_metric'] == metric)].shape[0])}
            ],
            'layout': {
                'title': '{} - {}'.format(var, metric),
                'xaxis': {'title': 'Date'},
                'yaxis': {'title': 'value'},
                'margin': {'l': 400, 'b':100, 't': 50, 'r': 400},
            }

        }
    )
    return plot

