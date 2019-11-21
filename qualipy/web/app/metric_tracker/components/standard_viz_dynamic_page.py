from functools import reduce
from collections import Counter

import dash_core_components as dcc
import numpy as np
import pandas as pd
import plotly.graph_objs as go

from qualipy.anomaly_detection import AnomalyModel


def create_value_count_area_chart(data, var):
    # data_values = data["value"].tolist()
    data_values = [(pd.Series(c) / pd.Series(c).sum()).to_dict() for c in data["value"]]
    traces = []
    unique_vals = reduce(lambda x, y: x.union(y), [set(i.keys()) for i in data_values])
    x = data["date"]
    for value in unique_vals:
        traces.append(
            dict(
                x=data["date"],
                y=[i.get(value, 0) for i in data_values],
                hoverinfo="x+y",
                mode="lines",
                line=dict(width=0.5),
                stackgroup="one",
                name=value,
            )
        )

    plot = dcc.Graph(
        id="value-count-graph-{}".format(var),
        figure={"data": traces, "layout": {"title": {"text": var}}},
    )
    return plot


def create_prop_change_list(data, var):
    data_values = [(pd.Series(c) / pd.Series(c).sum()).to_dict() for c in data["value"]]
    traces = []
    unique_vals = reduce(lambda x, y: x.union(y), [set(i.keys()) for i in data_values])
    potential_lines = []
    for cat in unique_vals:
        values = pd.Series([i.get(cat, 0) for i in data_values])
        running_means = values.rolling(window=5).mean()
        differences = values - running_means
        sum_abs = np.abs(differences).sum()
        potential_lines.append((cat, differences, sum_abs))
    potential_lines = sorted(potential_lines, key=lambda v: v[2], reverse=True)

    try:
        all_lines = pd.DataFrame({i[0]: i[1] for i in potential_lines})
        mod = AnomalyModel()
        outliers = mod.train_predict(all_lines.values[4:])
        outliers = np.concatenate([np.array([1, 1, 1, 1]), outliers])
        outliers = [0 if i == -1 else np.NaN for i in outliers]
    except:
        outliers = np.repeat(np.NaN, len(data_values))

    for line in potential_lines[:5]:
        traces.append(dict(x=data["date"], y=line[1], mode="lines", name=line[0]))
    traces.append(
        dict(
            x=data["date"],
            y=outliers,
            name="Outlier",
            mode="markers",
            marker=dict(color="rgb(164, 4, 4)", size=10),
        )
    )

    plot = dcc.Graph(
        id="prop-change-graph-{}".format(var),
        figure={
            "data": traces,
            "layout": {
                "title": {"text": "Difference in proportion over time (top 5) "}
            },
        },
    )
    return plot


def barchart_top_cats(data):
    counter = Counter(data.value.values[0])
    for vc in data.value.values[1:]:
        counter += Counter(vc)
    items = counter.items()
    x = [i[0] for i in items]
    y = [i[1] for i in items]
    plot = dcc.Graph(
        figure=go.Figure(
            data=[go.Bar(x=x, y=y)],
            layout=go.Layout(title="All Categories", xaxis={"type": "category"}),
        ),
        id="barchart-all-categories",
    )
    return plot
