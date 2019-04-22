import dash_core_components as dcc
import plotly.graph_objs as go


def heatmap(data, col, metric):
    data = data[(data['_name'] == col) & (data['_metric'] == metric)]
    value = data.value.iloc[0]
    column_two_name = data._arguments.iloc[0]
    trace = go.Heatmap(
        x=[str(i) for i in value['x']],
        y=[str(i) for i in value['y']],
        z=value['z']
    )
    plot = dcc.Graph(
        id='heatmap-{}-{}'.format(col, column_two_name),
        figure=go.Figure(
            data=[trace],
            layout=go.Layout(
                title='Heatmap-{}-{}'.format(col, column_two_name),
                height=600,
                width=800,
                xaxis={
                    'automargin': True,
                    # 'tickvals':
                },
                yaxis={
                    'automargin': True,
                    'tickvals': value['y']
                }
            )
        )
    )
    return plot

