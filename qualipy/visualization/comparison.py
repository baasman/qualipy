import pandas as pd
import numpy as np
import plotly.graph_objects as go
from qualipy.util import set_value_type
from plotly.subplots import make_subplots

import altair as alt


def comparison_trends(data, columns, metrics, show_column_in_name=False):
    df = data.copy()
    df = df[(df.column_name.isin(columns)) & (df.metric.isin(metrics))]
    df["metric_name"] = df.metric.astype(str) + np.where(
        df.arguments.isnull(), "", df.arguments
    )
    if show_column_in_name:
        df.metric_name = df.metric_name + "-" + df.column_name
    col_name = df.column_name.values[0]
    df = df.sort_values(["metric_name", "date"])
    fig = make_subplots(
        rows=df.metric_name.nunique() * 2,
        cols=1,
        shared_xaxes=True,
        shared_yaxes=False,
        vertical_spacing=0.1,
        subplot_titles=np.repeat(df.metric_name.unique().tolist(), 2),
    )
    count = 1
    for row, (name, group) in enumerate(df.groupby("metric_name"), start=1):
        x = group.date
        group = set_value_type(group.copy())
        for col in group.column_name.unique():
            x = group[group.column_name == col].date
            y = group[group.column_name == col].value.values
            fig.append_trace(
                go.Scatter(x=x, y=y, name=col), row=count, col=1,
            )
        count += 1
        for col in group.column_name.unique():
            mean = group[group.column_name == col].value.mean()
            std = group[group.column_name == col].value.std()
            x = group[group.column_name == col].date
            y = (group[group.column_name == col].value.values - mean) / std
            fig.append_trace(
                go.Scatter(x=x, y=y, name=col), row=count, col=1,
            )
        count += 1

    fig["layout"].update(
        title_text="Comparison between {}".format(
            "-".join(df.column_name.unique().tolist())
        ),
        showlegend=False,
    )
    fig.show()


def plot_batch_comparison(df1, df2):
    comp_column_name = df1.column_name.iloc[0]
    fig = make_subplots(
        rows=df1.metric.nunique(),
        cols=1,
        shared_xaxes=True,
        shared_yaxes=False,
        vertical_spacing=0.1,
        subplot_titles=df1.metric.unique(),
    )
    mins = []
    maxs = []
    for row, metric in enumerate(df1.metric.unique(), start=1):
        x1 = df1[df1.metric == metric].date
        y1 = df1[df1.metric == metric].value.values
        fig.append_trace(
            go.Scatter(x=x1, y=y1, name="project", mode="markers", opacity=0.8),
            row=row,
            col=1,
        )
        x2 = df2[df2.metric == metric].date
        y2 = df2[df2.metric == metric].value.values
        fig.append_trace(
            go.Scatter(x=x2, y=y2, name="project_other", mode="markers", opacity=0.8),
            row=row,
            col=1,
        )
        mins.append(max(x1.min(), x2.min()))
        maxs.append(min(x1.max(), x2.max()))
    min_date = min(mins)
    max_date = max(maxs)
    for row in range(1, df1.metric.nunique() + 1):
        x_axis_name = f"xaxis{'' if row == 1 else row}_range"
        fig["layout"].update({x_axis_name: [min_date, max_date]})
    fig["layout"].update(
        title_text=f"Comparison for {comp_column_name}", showlegend=False,
    )
    fig.show()


def comparison_trends_altair(data, columns, metrics, show_column_in_name=False):
    df = data.copy()
    df = df[(df.column_name.isin(columns)) & (df.metric.isin(metrics))]
    df["metric_name"] = df.metric.astype(str) + np.where(
        df.arguments.isnull(), "", df.arguments
    )
    if show_column_in_name:
        df.metric_name = df.metric_name + "-" + df.column_name
    col_name = df.column_name.values[0]
    comparison_column_name = " - ".join(columns)
    df = set_value_type(df)
    df = df.sort_values(["metric_name", "date"])

    df["mean_value"] = df.groupby(["column_name", "metric_name"]).value.transform(
        "mean"
    )
    df["std_value"] = df.groupby(["column_name", "metric_name"]).value.transform("std")
    df["standardized_value"] = (df.value - df.mean_value) / df.std_value
    for row, (name, group) in enumerate(df.groupby("metric_name"), start=1):

        chart = alt.Chart(group).properties(
            width=800,
            height=200,
            title=f"Comparison of values - {comparison_column_name} - {name}",
        )
        chart = chart.mark_line().encode(x="date:T", y="value:Q", color="column_name")

        std_chart = alt.Chart(group).properties(
            width=800, height=200, title="Standardized values"
        )
        std_chart = std_chart.mark_line().encode(
            x="date:T", y="standardized_value:Q", color="column_name"
        )

        plot = (
            alt.vconcat(chart, std_chart)
            .resolve_axis(y="shared")
            .configure_legend(orient="bottom")
        )
        plot.display()
