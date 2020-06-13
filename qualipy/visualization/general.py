import pandas as pd
import numpy as np
import plotly.graph_objects as go
from qualipy.util import set_value_type
from functools import reduce
from collections import Counter
from plotly.subplots import make_subplots
import altair as alt


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

    n_groups = y.shape[0] // 10
    n = y.shape[0] // n_groups
    y_groups = [y[i * n : (i + 1) * n] for i in range((len(y) + n - 1) // n)]
    x_groups = [x[i * n : (i + 1) * n] for i in range((len(x) + n - 1) // n)]
    nullable_string_groups = [
        nullable_strings[i * n : (i + 1) * n]
        for i in range((len(nullable_strings) + n - 1) // n)
    ]
    nullable_values_groups = [
        nullable_values[i * n : (i + 1) * n]
        for i in range((len(nullable_values) + n - 1) // n)
    ]
    colors_groups = [
        colors[i * n : (i + 1) * n] for i in range((len(colors) + n - 1) // n)
    ]
    for x, y, nullable_strings, nullable_values, colors in zip(
        x_groups,
        y_groups,
        nullable_string_groups,
        nullable_values_groups,
        colors_groups,
    ):
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


def row_count_view(data, anom_data=None, columns=None):
    if columns is not None:
        data = data[data.column_name.isin(columns)]
    data = data[
        (data["column_name"].str.contains("rows")) & (data["metric"] == "count")
    ]
    data.value = data.value.astype(float)

    if anom_data is not None:
        anom_data = anom_data[
            (anom_data["column_name"].str.contains("rows"))
            & (anom_data["metric"] == "count")
        ]
        data = data.merge(
            anom_data[["column_name", "metric", "batch_name", "value"]].rename(
                columns={"value": "value_anom"}
            ),
            on=["column_name", "metric", "batch_name"],
            how="left",
        ).drop_duplicates()

    row_runs = data.column_name.unique()
    n_groups = row_runs.shape[0] // 10
    n = row_runs.shape[0] // n_groups
    groups = [
        row_runs[i * n : (i + 1) * n] for i in range((len(row_runs) + n - 1) // n)
    ]
    dfs = []
    for row_vars in groups:
        new_df = data[data.column_name.isin(row_vars)].copy()
        dfs.append(new_df)
    for idx, df in enumerate(dfs):
        title = "Row counts over time" if idx == 0 else None
        row_runs = df.column_name.unique()
        fig = make_subplots(
            rows=row_runs.shape[0], cols=1, shared_xaxes=True, shared_yaxes=False,
        )
        for row, (name, group) in enumerate(df.groupby("column_name"), start=1):
            scat = go.Scatter(x=group.date, y=group.value.values, name=name)
            fig.append_trace(scat, row=row, col=1)
            outliers = go.Scatter(
                x=group.date,
                y=group.value_anom.values,
                mode="markers",
                marker=dict(color="red", size=10),
                showlegend=False,
            )
            fig.append_trace(outliers, row=row, col=1)

        fig["layout"].update(height=900, width=1000, title=title)
        fig.show()


def missing_by_column_bar_altair(data, schema=None, show_notebook=True):
    data = set_value_type(data)
    data = data.sort_values("value", ascending=False)
    mean_missing = data.groupby("column_name").value.mean().reset_index()
    base = alt.Chart(mean_missing).properties(
        title="Mean Percentage Missing", width=800
    )
    bars = base.mark_bar().encode(
        y=alt.Y("column_name:N"), x=alt.X("value:Q", scale=alt.Scale(domain=[0, 1]))
    )
    if show_notebook:
        bars.display()
    else:
        return bars


def row_count_view_altair(
    data, anom_data=None, columns=None, only_anomaly=True, show_notebook=True
):
    if columns is not None:
        data = data[data.column_name.isin(columns)]
    data = data[
        (data["column_name"].str.contains("rows")) & (data["metric"] == "count")
    ]
    data.value = data.value.astype(float)

    if anom_data is not None:
        anom_data = anom_data[
            (anom_data["column_name"].str.contains("rows"))
            & (anom_data["metric"] == "count")
        ]
        data = data.merge(
            anom_data[["column_name", "metric", "batch_name", "value"]].rename(
                columns={"value": "value_anom"}
            ),
            on=["column_name", "metric", "batch_name"],
            how="left",
        ).drop_duplicates()
        columns_with_anoms = data[data.value_anom.notnull()].column_name.unique()
        if only_anomaly:
            data = data[data.column_name.isin(columns_with_anoms)]

    all_lines = []
    for _, (name, df) in enumerate(data.groupby("column_name")):
        line = (
            alt.Chart(df)
            .mark_line()
            .encode(x=alt.X("date:T"), y=alt.Y("value:Q"))
            .properties(height=200, width=800, title=name)
        )
        line = alt.layer(
            line,
            alt.Chart(df)
            .mark_point(color="red", size=50)
            .encode(x=alt.X("date:T"), y=alt.Y("value_anom:Q")),
        )
        all_lines.append(line)
    full_chart = alt.vconcat(*all_lines[:100]).resolve_axis(x="shared")
    if show_notebook:
        full_chart.display()
    else:
        return full_chart
