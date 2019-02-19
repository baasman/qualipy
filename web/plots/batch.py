import dash_core_components as dcc


def compare_batch_with_rest(data, batch, metric):
    if batch == 'all':
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
        id='var_from_mean-{}-{}'.format(all, metric),
        figure={
            'data': [
                {'x': data_cur['_metric'].unique(), 'y': current, 'type':'bar', 'name': str(batch)},
                {'x': data_cur['_metric'].unique(), 'y': old, 'type':'bar', 'name': 'other'},
            ],
            'layout': {
                'title': 'batch(es) {} versus the rest'.format(batch),
                'yaxis': {'title': 'value'},
                'xaxis': {'title': 'metric'},
                'margin': {'l': 400, 'b':100, 't': 50, 'r': 400},
            }

        }
    )
    return plot
