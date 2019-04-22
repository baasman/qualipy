import dash_core_components as dcc
import plotly.graph_objs as go
import pandas as pd


def heatmap(data, col, metric):
    data = data[(data['_name'] == col) & (data['_metric'] == metric)]
    print(data.value.iloc[0])

    trace = go.Heatmap(**data.value.iloc[0])


def bar_plot_missing(data, metric, schema):
    df = data[data['_metric'] == metric].copy()
    df.value = df.value.astype(float)
    df = df.sort_values('value')
    y = df.groupby('_name').value.mean()
    x = y.index
    nullable = ['value: {}, null: {}'.format(round(value, 2), str(schema[i]['nullable']))
                for value, i in zip(y, x)]
    colors = ['rgb(11, 57, 142)' if null else 'rgb(191, 11, 38)' for null in nullable]

    plot = dcc.Graph(
        id='percentage_missing',
        figure={
            'data': [
                {
                    'y': x,
                    'x': y,
                    'type':'bar',
                    'width': .5,
                    'orientation': 'h',
                    'hoverinfo': 'text',
                    'text': nullable,
                    'marker': {
                        'color': colors
                    }
                },
            ],
            'layout': {
                'title': {'text': 'Percentage Missing'},
                'height': 800,
                'width': 800,
                'xaxis': {
                    'title': 'Percentage Missing',
                    'range': [0, 1],
                },
                'yaxis': {
                    'automargin': True
                },
            }

        }
    )
    return plot


def histogram(data, var, metric):
    x = data[(data['_name'] == var) &
              (data['_metric'] == metric)]['value']
    plot = dcc.Graph(
        id='histogram-{}-{}'.format(var, metric),
        figure={
            'data': [
                {'x': x, 'type':'histogram', 'name': var, 'bins': 20},
            ],
            'layout': {
                'yaxis': {'title': var},
                'xaxis': {'title': 'metric'},
            }

        }
    )
    return plot


def compare_batch_with_rest(data, batch):
    if batch == 'all':
        # batch = data['_date'].values[-1]
        # data_cur = data[data['_date'] == batch]
        # data_old = data[data['_date'] != batch]
        pass
    else:
        data_cur = data[data['_date'].isin(batch)]
        data_old = data[~data['_date'].isin(batch)]
    unique_metrics = data_cur['_metric'].unique()
    current = [data_cur[data_cur['_metric'] == var].value.values[0] for var in unique_metrics]
    old = [data_old[data_old['_metric'] == var].value.mean() for var in unique_metrics]
    plot = dcc.Graph(
        id='var_from_mean-{}'.format(batch),
        figure={
            'data': [
                {'x': data_cur['_metric'].unique(), 'y': current, 'type':'bar', 'name': str(batch)},
                {'x': data_cur['_metric'].unique(), 'y': old, 'type':'bar', 'name': 'other'},
            ],
            'layout': {
                'title': 'Difference from mean - batch(es) {} versus the rest'.format(batch),
                'yaxis': {'title': 'value'},
                'xaxis': {'title': 'metric'},
            }

        }
    )
    return plot
def dot_plot(data, batch):
    if batch == 'last':
        batch = data['_date'].values[-1]
        data_cur = data[data['_date'] == batch]
        data_old = data[data['_date'] != batch]
    else:
        data_cur = data[data['_date'].isin(batch)]
        data_old = data[~data['_date'].isin(batch)]
    unique_metrics = data_cur['_metric'].unique()
    current = [data_cur[data_cur['_metric'] == var].value.values[0] for var in unique_metrics]
    old = [data_old[data_old['_metric'] == var].value.mean() for var in unique_metrics]
    plot = dcc.Graph(
        id='dot plot',
        figure={
            'data': [
                {'x': current,
                 'y': unique_metrics,
                 "marker": {"color": "pink", "size": 12},
                 'type':'scatter',
                 'mode': 'markers',
                 'name': batch},
                {'x': old,
                 'y': unique_metrics,
                 "marker": {"color": "blue", "size": 12},
                 'type':'scatter',
                 'mode': 'markers',
                 'name': 'rest'},
            ],
            'layout': {
                'title': 'Difference from mean - batch(es) {} versus the rest'.format(batch),
                'yaxis': {'title': 'value'},
                'xaxis': {'title': 'metric'},
                'margin': {'l': 400, 'b':100, 't': 50, 'r': 400},
            }

        }
    )
    return plot

