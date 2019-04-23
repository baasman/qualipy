import dash_core_components as dcc
import plotly.graph_objs as go


def heatmap(data, col, metric):
    data = data[(data['_name'] == col) & (data['_metric'] == metric)]
    value = data.value.iloc[0]
    column_two_name = data._arguments.iloc[0]
    trace = go.Heatmap(
        x=['cat-{}'.format(i) for i in value['x']],
        y=['cat-{}'.format(i) for i in value['y']],
        z=value['z']
    )
    plot = dcc.Graph(
        id='heatmap-{}-{}'.format(col, column_two_name),
        figure=go.Figure(
            data=[trace],
            layout=go.Layout(
                title='{}-{}-{}'.format(metric, col, column_two_name),
                height=600,
                width=800,
                xaxis={
                    'automargin': True,
                    'title': column_two_name
                },
                yaxis={
                    'automargin': True,
                    'title': col
                }
            )
        )
    )
    return plot

