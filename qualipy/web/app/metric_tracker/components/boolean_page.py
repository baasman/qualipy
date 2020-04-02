import dash_core_components as dcc
import numpy as np
import dash_table

from functools import reduce

from qualipy.util import set_value_type


def error_check_table(data):
    return dash_table.DataTable(
        id="error-table",
        columns=[{"name": i, "id": i} for i in data.columns],
        data=data.to_dict("rows"),
        sort_action="native",
        css=[
            {
                "selector": ".dash-cell div.dash-cell-value",
                "rule": "display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;",
            }
        ],
        style_data={"whiteSpace": "normal", "maxWidth": 300, "margin": {"b": 100}},
    )


def boolean_plot(data):
    x = data["date"]
    batch_names = data["batch_name"]
    traces = []
    unique_vars = data["column_name"].unique()
    data = set_value_type(data)
    for var in unique_vars:
        df = data[data["column_name"] == var]
        metrics = df.metric.unique()
        for idx, metric in enumerate(metrics, 1):
            vals = data[data["metric"] == metric].value.values
            trues = [idx if i else np.NaN for i in vals]
            falses = [np.NaN if i else idx for i in vals]
            traces.append(
                dict(
                    x=x,
                    y=trues,
                    mode="markers",
                    name="True",
                    text=x,
                    marker=dict(size=8, color="rgb(62, 239, 52)"),
                    showlegend=True if idx == 1 else False,
                )
            )
            traces.append(
                dict(
                    x=x,
                    y=falses,
                    mode="markers",
                    text=x,
                    name="False",
                    marker=dict(size=8, color="rgb(234, 11, 11)"),
                    showlegend=True if idx == 1 else False,
                )
            )
    plot = dcc.Graph(
        id="uniqueness-plot",
        figure={
            "data": traces,
            "layout": {
                "title": {"text": "Check metrics - over time"},
                # 'height': 400,
                "width": 800,
                "yaxis": {
                    "showticklabels": True,
                    "showgrid": True,
                    "zeroline": False,
                    "tickvals": [1, metrics.shape[0]],
                    "ticktext": metrics,
                    "automargin": True,
                },
                "xaxis": {"tickvals": batch_names},
                "hoverinfo": "text",
            },
        },
    )
    return plot
