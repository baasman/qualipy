import dash_core_components as dcc
import numpy as np
from plotly.subplots import make_subplots
import plotly.graph_objs as go

from qualipy.util import set_value_type
from qualipy.anomaly_detection import LoadedModel

import traceback


def all_trends(data, show_column_in_name=False):
    df = data.copy()
    df["metric_name"] = df.metric.astype(str) + np.where(
        df.arguments.isnull(), "", df.arguments
    )
    if show_column_in_name:
        df.metric_name = df.metric_name + "-" + df.column_name
    col_name = df.column_name.values[0]
    fig = make_subplots(
        rows=df.metric_name.nunique(),
        cols=1,
        shared_xaxes=True,
        shared_yaxes=False,
        vertical_spacing=0.1,
        subplot_titles=df.metric_name.unique().tolist(),
    )
    for row, (name, group) in enumerate(df.groupby("metric_name"), start=1):
        x = group.date
        group = set_value_type(group)
        try:
            mod = LoadedModel()
            mod.load(
                "vmstat_pc_analysis",
                group.column_name.values[0],
                group.metric.values[0],
                group.arguments.values[0],
            )
            preds = mod.predict(group.value.values.reshape((1, -1)))
            print(preds)
        except:
            print(traceback.format_exc())
            pass
        fig.append_trace(go.Scatter(x=x, y=group.value.values), row=row, col=1)

    fig["layout"].update(
        title_text="All Numerical Trends - {}".format(col_name), showlegend=False
    )

    plot = dcc.Graph(id="all-num-aggs-{}".format(col_name), figure=fig)
    return plot


def create_trend_line(data, var, metric):
    title = "{}_{}".format(var, data.arguments.iloc[0])
    main_line = data.value
    mean = main_line.mean()
    median = main_line.median()
    std = main_line.std()
    mean_line = np.repeat(mean, main_line.shape[0])
    median_line = np.repeat(median, main_line.shape[0])
    std_line_lower = np.repeat(mean - (2 * std), main_line.shape[0])
    std_line_higher = np.repeat(mean + (2 * std), main_line.shape[0])

    x_axis = data["date"]

    plot = dcc.Graph(
        id="num-data-graph-{}-{}".format(title, metric),
        figure={
            "data": [
                {
                    "y": main_line,
                    "x": x_axis,
                    "name": title,
                    "marker": {
                        "line": "rgba(244, 66, 66)",
                        "dash": "dot",
                        "width": 1.5,
                    },
                },
                {
                    "y": mean_line,
                    "x": x_axis,
                    "mode": "lines",
                    "name": "mean-{}".format(title),
                    "line": {
                        "color": "rgba(244, 66, 66, .7)",
                        "width": 1,
                        "dash": "dash",
                    },
                },
                {
                    "y": median_line,
                    "x": x_axis,
                    "name": "median-{}".format(title),
                    "mode": "lines",
                    "line": {
                        "color": "rgba(244, 66, 232, .7)",
                        "width": 1,
                        "dash": "dash",
                    },
                },
                {
                    "y": std_line_lower,
                    "x": x_axis,
                    "name": "-2 std - {}".format(title),
                    "mode": "lines",
                    "line": {
                        "color": "rgba(161, 244, 66, .7)",
                        "width": 1,
                        "dash": "dash",
                    },
                },
                {
                    "y": std_line_higher,
                    "x": x_axis,
                    "name": "+2 std - {}".format(title),
                    "mode": "lines",
                    "line": {
                        "color": "rgba(66, 244, 188, .7)",
                        "width": 1,
                        "dash": "dash",
                    },
                },
            ],
            "layout": {
                "yaxis": {"title": "value"},
                "xaxis": dict(
                    rangeselector=dict(
                        buttons=list(
                            [
                                dict(
                                    count=1,
                                    label="1m",
                                    step="month",
                                    stepmode="backward",
                                ),
                                dict(
                                    count=6,
                                    label="6m",
                                    step="month",
                                    stepmode="backward",
                                ),
                                dict(step="all"),
                            ]
                        )
                    ),
                    rangeslider=dict(visible=True),
                    type="date",
                ),
            },
        },
    )
    return plot


def histogram(data, var, metric):
    x = data.value
    plot = dcc.Graph(
        id="histogram-{}-{}".format(var, metric),
        figure={
            "data": [{"x": x, "type": "histogram", "name": var, "bins": 20}],
            "layout": {"yaxis": {"title": var}, "xaxis": {"title": "metric"}},
        },
    )
    return plot
