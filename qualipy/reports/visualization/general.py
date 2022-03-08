import pandas as pd
import numpy as np
from qualipy.util import set_value_type
from functools import reduce
from collections import Counter
import altair as alt


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


def row_count_bar_latest(data):
    data = data[
        (data["column_name"].str.contains("rows")) & (data["metric"] == "count")
    ]
    data.value = data.value.astype(float)
    data = data.sort_values(["metric_id", "date"])
    data["value_diff"] = data.groupby("metric_id").value.diff()
    data = data.groupby("metric_id").nth(-1)
    batch_name = data.batch_name.iloc[0]
    data = data[["date", "column_name", "value", "value_diff"]]
    bars = (
        alt.Chart(data)
        .mark_bar()
        .encode(
            x=alt.X("column_name:N"),
            y=alt.Y("value_diff:Q"),
            tooltips=["column_name", "value_diff"],
        )
        .properties(width=800, title=f"Total count for {batch_name}")
    )
    text = bars.mark_text(align="center", baseline="middle", dy=-6).encode(
        text="value_diff:Q"
    )
    chart = alt.layer(bars, text)
    return chart


def row_count_summary(data):
    data = data[
        (data["column_name"].str.contains("rows")) & (data["metric"] == "count")
    ]
    data.value = data.value.astype(float)
    data = data.sort_values(["metric_id", "date"])
    data["value_change"] = data.groupby("metric_id").value.diff()
    data["percentage_change"] = data.groupby("metric_id").value.pct_change()
    data = data.melt(
        id_vars=["column_name", "date"],
        value_vars=["percentage_change", "value_change"],
    )
    trends = (
        alt.Chart(data)
        .mark_line(point=True)
        .encode(
            x="date:T",
            y=alt.Y("value:Q"),
            color="column_name",
            tooltip=["value", "column_name"],
        )
        .properties(width=800, height=200)
    )
    trends = trends.facet(
        row=alt.Row("variable:N", title=None), title="Row Count Summary"
    ).resolve_scale(y="independent")
    trends = trends.configure_title(dx=450)
    return trends


def missing_bar_latest(data):
    data = data[(data["metric"] == "perc_missing")].copy()
    data.value = data.value.astype(float).round(3)
    data = data.sort_values(["metric_id", "date"])
    data = data.groupby("metric_id").nth(-1)
    batch_name = data.batch_name.iloc[0]
    data = data[["date", "column_name", "value"]]
    bars = (
        alt.Chart(data)
        .mark_bar()
        .encode(y=alt.Y("column_name:N"), x=alt.X("value:Q"))
        .properties(width=800, title=f"Total Missingness for {batch_name}")
    )
    text = bars.mark_text(align="center", baseline="middle", dx=-10).encode(
        text="value:Q"
    )
    chart = alt.layer(bars, text)
    return chart
