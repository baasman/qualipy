import dash_core_components as dcc
import plotly.graph_objs as go
from plotly import tools
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


def create_simple_line_plot_subplots(data):
    data1 = data[(data['_name'] == 'rows') &
                (data['_metric'] == 'count')]
    data2 = data[(data['_name'] == 'columns') &
                 (data['_metric'] == 'count')]
    data_values1 = data1['value'].values
    data_values2 = data2['value'].values
    x = data1['_date']

    fig = tools.make_subplots(rows=2, cols=1,
                              shared_xaxes=True, shared_yaxes=False,
                              vertical_spacing=.01)

    trace1 = go.Scatter(
        x=x,
        y=data_values1,
        name='rows'
    )
    trace2 = go.Scatter(
        x=x,
        y=data_values2,
        name='columns'
    )
    fig.append_trace(trace1, row=1, col=1)
    fig.append_trace(trace2, row=2, col=1)
    fig['layout'].update(height=600, width=800,
                         title='Rows and Columns over time')

    plot = dcc.Graph(
        id='simple-line-plot-{}'.format('rows-columns'),
        figure=fig
    )
    return plot


def create_type_plots(data, schema):
    dtype_data = data[(data['_metric'] == 'dtype')]
    x_index = dtype_data['_date']

    uniques = dtype_data.groupby('_name').apply(lambda g: g.drop_duplicates('value').shape[0])
    uniques = uniques[uniques > 1].index

    try:
        if uniques.shape[0] > 0:
            fig = tools.make_subplots(rows=len(uniques), cols=1,
                                      shared_xaxes=True, shared_yaxes=False,
                                      vertical_spacing=.10, subplot_titles=uniques.values)
            number_of_lines = []
            for idx, var in enumerate(uniques, 1):
                values = dtype_data[dtype_data['_name'] == var]['value'].values
                n = values.shape[0]
                unique_vals = np.unique(values)
                lines = []
                # convert_dict = {k: i + 1 for i, k in zip(range(unique_vals.shape[0]), unique_vals)}
                convert_dict = {k: 1 for i, k in zip(range(unique_vals.shape[0]), unique_vals)}
                for line in unique_vals:
                    l = [line if line == values[idx] else np.NaN
                         for idx, dtype in enumerate(range(n))]
                    l = [convert_dict.get(i, np.NaN) for i in l]
                    lines.append({
                        'name': line,
                        'data': l
                    })
                number_of_lines.append(len(lines))

                for line in lines:
                    data = line['data']
                    name = line['name']
                    fig.append_trace(go.Scatter(
                        x=x_index,
                        y=data,
                        mode='lines',
                        name=name,
                        line=dict(
                            width=8
                        ),
                        showlegend=True
                        # showlegend=True if idx == 1 else False,
                    ), row=idx, col=1)
    except ValueError:
        print('Problem with too many subplots')
        fig = None

    if uniques.shape[0] > 0:
        fig['layout'].update(height=300 * uniques.shape[0], width=800,
                             title='Data types of variables over time')
        fig['layout']['xaxis'].update(showticklabels=True, showgrid=True, zeroline=False)
        for idx, var in enumerate(uniques, 1):
            yaxis_name = 'yaxis{}'.format(idx) if idx != 1 else 'yaxis'
            y_range = list(range(1, number_of_lines[idx - 1] + 1))
            fig['layout'][yaxis_name].update(
                showticklabels=False,
                showgrid=True,
                zeroline=False,
                # range=[y_range[0] - .5, y_range[-1] + .5],
                range=[.75, 1.25],
            )

        plot = dcc.Graph(
            id='type-line-plot',
            figure=fig
        )
        return plot
    else:
        return dcc.Markdown('''
            All columns have had the same dtype over time
        ''')


def create_unique_columns_plot(data):
    data = data[(data['_metric'] == 'is_unique')]
    x = data['_date']
    traces = []
    unique_vars = data['_name'].unique()
    for idx, var in enumerate(unique_vars, 1):
        vals = data[data['_name'] == var].value.values
        uniques = [idx if i else np.NaN for i in vals]
        non_uniques = [np.NaN if i else idx for i in vals]
        traces.append(
            dict(
                x=x,
                y=uniques,
                mode='lines',
                name='unique',
                line=dict(
                    width=8,
                    color='rgb(62, 239, 52)'
                ),
                showlegend=True if idx == 1 else False

            )
        )
        traces.append(
            dict(
                x=x,
                y=non_uniques,
                mode='lines',
                name='not-unique',
                line=dict(
                    width=8,
                    color='rgb(234, 11, 11)'
                ),
                showlegend=True if idx == 1 else False
            )
        )
    plot = dcc.Graph(
        id='uniqueness-plot',
        figure={
            'data': traces,
            'layout': {
                'title': {'text': 'Unique-ness checks'},
                'height': 400,
                'width': 800,
                'yaxis': {
                    'showticklabels': True,
                    'showgrid': True,
                    'zeroline': False,
                    'tickvals': [1, unique_vars.shape[0]],
                    'ticktext': unique_vars
                }
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
