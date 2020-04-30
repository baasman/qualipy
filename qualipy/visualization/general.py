import pandas as pd
import numpy as np
import plotly.graph_objects as go
from qualipy.anomaly_detection import (
    LoadedModel,
    anomaly_data_all_projects,
    AnomalyModel,
)
from qualipy.util import set_value_type
from functools import reduce
from collections import Counter
from plotly.subplots import make_subplots


def missing_by_column_bar(data, schema):
    data.value = data.value.astype(float)
    data = data.sort_values("value")
    y = data.groupby("column_name").value.mean()
    x = y.index
    nullable_strings = [
        "value: {}, null: {}".format(round(value, 2), str(schema[i]["nullable"]))
        for value, i in zip(y, x)
    ]
    nullable_values = [
        {"value": round(value, 2), "null": schema[i]["nullable"]}
        for value, i in zip(y, x)
    ]
    colors = [
        "rgb(11, 57, 142)" if null["null"] and null["value"] > 0 else "rgb(191, 11, 38)"
        for null in nullable_values
    ]
    plot = go.Figure(
        data=[
            go.Bar(
                y=x,
                x=y,
                width=0.5,
                orientation="h",
                hoverinfo="text",
                text=nullable_strings,
                marker={"color": colors},
            )
        ],
        layout={
            "title": {"text": "Percentage Missing"},
            "width": 1000,
            "xaxis": {
                "title": "Percentage Missing",
                "range": [0, 1],
                "automargin": True,
            },
            "yaxis": {"automargin": True},
            "bargap": 0.5,
        },
    )
    plot.show()


def row_count_view(data):
    data = data[
        (data["column_name"].str.contains("rows")) & (data["metric"] == "count")
    ]
    data.value = data.value.astype(float)
    row_runs = data.column_name.unique()

    fig = make_subplots(
        rows=row_runs.shape[0],
        cols=1,
        shared_xaxes=True,
        shared_yaxes=False,
        vertical_spacing=0.01,
    )
    for row, (name, group) in enumerate(data.groupby("column_name"), start=1):
        scat = go.Scatter(x=group.date, y=group.value.values, name=name)
        fig.append_trace(scat, row=row, col=1)

    fig["layout"].update(height=700, width=800, title="Row Counts over time")
    fig.show()


def unique_columns_check(data, schema):
    x = data["batch_name"]
    traces = []
    unique_vars = data["column_name"].unique()
    for idx, var in enumerate(unique_vars, 1):
        vals = data[data["column_name"] == var].value.values
        uniques = [idx if i else np.NaN for i in vals]
        non_uniques = [np.NaN if i else idx for i in vals]
        traces.append(
            go.Scatter(
                x=x,
                y=uniques,
                mode="markers",
                name="unique",
                marker=dict(size=8, color="rgb(62, 239, 52)"),
                showlegend=True if idx == 1 else False,
            )
        )
        traces.append(
            go.Scatter(
                x=x,
                y=non_uniques,
                mode="markers",
                name="not-unique",
                marker=dict(size=8, color="rgb(234, 11, 11)"),
                showlegend=True if idx == 1 else False,
            )
        )
    plot = go.Figure(
        data=traces,
        layout={
            "title": {"text": "Unique-ness checks"},
            "height": 400,
            "width": 800,
            "yaxis": {
                "showticklabels": True,
                "showgrid": True,
                "zeroline": False,
                "tickvals": [1, unique_vars.shape[0]],
                "ticktext": unique_vars,
                "automargin": True,
            },
            "xaxis": {"tickvals": x},
        },
    )
    plot.show()
