import dash_core_components as dcc
import plotly.graph_objs as go
import numpy as np

from functools import reduce


def create_trend_line(data, var, metric):

    main_line = data[(data['_name'] == var) &
                               (data['_metric'] == metric)]['value']
    mean = main_line.mean()
    median = main_line.median()
    std = main_line.std()
    mean_line = np.repeat(mean, main_line.shape[0])
    median_line = np.repeat(median, main_line.shape[0])
    std_line_lower = np.repeat(mean - (2 * std), main_line.shape[0])
    std_line_higher = np.repeat(mean + (2 * std), main_line.shape[0])

    x_axis = data[(data['_name'] == var) &
         (data['_metric'] == metric)]['_date']

    plot = dcc.Graph(
        id='num-data-graph-{}-{}'.format(var, metric),
        figure={
            'data': [
                {
                    'y': main_line,
                    'x': x_axis,
                    'name': var,
                    'marker': {
                        'line': 'rgba(244, 66, 66)',
                        'dash': 'dot',
                        'width': 1.5,
                    }
                },
                {
                    'y': mean_line,
                    'x': x_axis,
                    'mode': 'lines',
                    'name': 'mean-{}'.format(var),
                    'line': {
                        'color': 'rgba(244, 66, 66, .7)',
                        'width': 1,
                        'dash': 'dash'
                    },
                },
                {
                    'y': median_line,
                    'x': x_axis,
                    'name': 'median-{}'.format(var),
                    'mode': 'lines',
                    'line': {
                        'color': 'rgba(244, 66, 232, .7)',
                        'width': 1,
                        'dash': 'dash'
                    },
                },
                {
                    'y': std_line_lower,
                    'x': x_axis,
                    'name': '-2 std - {}'.format(var),
                    'mode': 'lines',
                    'line': {
                        'color': 'rgba(161, 244, 66, .7)',
                        'width': 1,
                        'dash': 'dash'
                    },
                },
                {
                    'y': std_line_higher,
                    'x': x_axis,
                    'name': '+2 std - {}'.format(var),
                    'mode': 'lines',
                    'line': {
                        'color': 'rgba(66, 244, 188, .7)',
                        'width': 1,
                        'dash': 'dash'
                    },
                },
            ],
            'layout': {
                'yaxis': {'title': 'value'},
                'xaxis': dict(
                rangeselector=dict(
                    buttons=list([
                        dict(count=1,
                             label='1m',
                             step='month',
                             stepmode='backward'),
                        dict(count=6,
                             label='6m',
                             step='month',
                             stepmode='backward'),
                        dict(step='all')
                    ])
                ),
                rangeslider=dict(
                    visible=True
                ),
                type='date'
    )

            }

        }
    )
    return plot


def create_value_count_area_chart(data, var, metric):
    data = data[(data['_name'] == var) &
               (data['_metric'] == metric)]
    data_values = data['value'].tolist()
    traces = []
    unique_vals = reduce(lambda x, y: x.union(y), [set(i.keys()) for i in data_values])
    x = data['_date']
    for value in unique_vals:
        traces.append(
            dict(
                x=x,
                y=[i.get(value, 0) for i in data_values],
                hoverinfo='x+y',
                mode='lines',
                line=dict(width=0.5),
                stackgroup='one',
                name=value
            )
        )

    plot = dcc.Graph(
        id='value-count-graph-{}'.format(var),
        figure={
            'data': traces,
            'layout': {
                'title': {'text': var},
            }

        }
    )
    return plot


def create_simple_line_plot(data, var, metric):
    data = data[(data['_name'] == var) &
                (data['_metric'] == metric)]
    data_values = data['value'].values
    x = data['_date']
    trace = dict(
        x=x,
        y=data_values
    )

    plot = dcc.Graph(
        id='simple-line-plot-{}'.format(var),
        figure={
            'data': [trace],
            'layout': {
                'title': {'text': var},
            }

        }
    )
    return plot
