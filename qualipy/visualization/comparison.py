import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import altair as alt

from qualipy.util import set_value_type

from collections import Counter


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


def plot_diffs_altair(data_1, data_2, m1, m2, time_freq="1D", show_notebook=True):
    df1 = data_1[data_1.metric_id == m1].copy()
    df1 = set_value_type(df1)
    df1["standardized"] = (df1.value - df1.value.mean()) / df1.value.std()
    df2 = data_2[data_2.metric_id == m2].copy()
    df1 = set_value_type(df1)
    df2.value = df2.value.astype(int) + 20
    df2["standardized"] = (df2.value - df2.value.mean()) / df2.value.std()

    df = pd.concat([df1, df2])

    diff_df = df.set_index("date")
    resampled = (
        diff_df.groupby("metric_id")
        .value.resample(rule=time_freq)
        .sum()
        .reset_index()
        .pivot(columns="metric_id", index="date")
    )
    resampled.columns = [i[1] for i in resampled.columns]
    resampled["difference"] = resampled[m2] - resampled[m1]
    resampled = resampled.reset_index()

    chart = alt.Chart(df).properties(width=800, height=200, title=f"{m1} - {m2}")
    chart = chart.mark_line().encode(x="date:T", y="value:Q", color="metric_id")

    std_chart = alt.Chart(df).properties(width=800, height=100)
    std_chart = std_chart.mark_line().encode(
        x="date:T", y="standardized:Q", color="metric_id"
    )

    diff_chart = alt.Chart(resampled).properties(width=800, height=100)
    diff_chart = diff_chart.mark_line().encode(x="date:T", y="difference:Q")

    plot = alt.vconcat(chart, std_chart, diff_chart).resolve_axis(y="shared")
    if show_notebook:
        plot.display()
    else:
        return plot


def get_counts(data, metric_id, top_n=20):
    counter = Counter(data.value.values[0])
    for vc in data.value.values[1:]:
        counter += Counter(vc)
    items = counter.most_common()
    x = [i[0] for i in items][:top_n]
    y = [i[1] for i in items][:top_n]
    counts = pd.DataFrame({"category": x, "value": y})
    counts["metric"] = metric_id
    return counts


def bar_chart_comparison_altair(data: list, metrics: list, top_n=10, only_overlapping=False, show_notebook=True):
    counts = []
    for df, metric in zip(data, metrics):
        df_ = df[df.metric_id == metric].copy()
        df_ = set_value_type(df_)
        counts.append(get_counts(df_, metric))

    df = pd.concat(counts)

    title = ' - '.join(metrics)
    chart = alt.Chart(df).properties(height=50, title=title)
    chart = chart.mark_bar().encode(y=alt.Y('metric:N', axis=alt.Axis(title=None, labels=False)), x=alt.X('value:Q'), 
                                    row=alt.Row('category:N', header=alt.Header(labelAngle=0, labelAlign='left')), color='metric:N')
    plot = chart.configure_view(strokeOpacity=0)
    if show_notebook:
        plot.display()
    else:
        return plot
