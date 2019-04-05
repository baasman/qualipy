import dash_core_components as dcc


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

