import dash_core_components as dcc


def create_trend_line(data, var, metric):
    plot = dcc.Graph(
        id='num-data-graph-{}-{}'.format(var, metric),
        figure={
            'data': [
                {'y': data[(data['_name'] == var) &
                               (data['_metric'] == metric)]['value'],
                 'x': data[(data['_name'] == var) &
                                     (data['_metric'] == metric)]['_date']
                 }
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

