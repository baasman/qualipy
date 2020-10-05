import traceback
import os
import json
from functools import reduce
from collections import Counter
import warnings

import qualipy
from qualipy.util import set_value_type
from qualipy.util import set_title_name

import pandas as pd
import numpy as np
import altair as alt
import banpei


def trend_line_altair(
    trend_data: pd.DataFrame,
    var_name: str,
    config_dir: str,
    project_name: str,
    anom_data: pd.DataFrame,
    point: bool = True,
    sst: int = 30,
    display_notebook=True,
    add_diff=None,
    n_steps=20,
):
    args = trend_data.arguments.iloc[0]
    args = f"_{args}" if args is not None else ""
    metric_name = trend_data.metric.iloc[0]
    if anom_data.shape[0] > 0:
        trend_data = trend_data.merge(
            anom_data[["column_name", "batch_name", "value"]].rename(
                columns={"value": "anom_val"}
            ),
            how="left",
            on=["column_name", "batch_name"],
        )
        trend_data["anom_val"] = trend_data["anom_val"].astype(float)
    else:
        trend_data["anom_val"] = np.NaN

    trend_data["2std_plus_line"] = trend_data.value.mean() + (
        2 * trend_data.value.std()
    )
    trend_data["2std_minus_line"] = trend_data.value.mean() - (
        2 * trend_data.value.std()
    )

    trend_data["rolling_mean"] = trend_data.value.rolling(n_steps).mean()
    trend_data["rolling_deviation"] = 2 * trend_data.value.rolling(n_steps).std()

    trend_data["lower"] = trend_data.rolling_mean - trend_data.rolling_deviation
    trend_data["upper"] = trend_data.rolling_mean + trend_data.rolling_deviation

    trend_data["mean_line"] = trend_data.value.mean()
    trend_data["median_line"] = trend_data.value.median()
    td = trend_data[
        [
            "date",
            "value",
            "mean_line",
            "median_line",
        ]
    ].melt("date")

    min_y = min(trend_data.value.min() - 0.001, trend_data["lower"].min() - 0.001)
    max_y = max(trend_data.value.max() + 0.001, trend_data["upper"].max() + 0.001)
    title = set_title_name(trend_data)
    base = alt.Chart(td[["date", "value", "variable"]]).properties(
        title=title, width=800
    )
    value_line = base.mark_line(point=point).encode(
        x=alt.X("date:T"),
        y=alt.Y("value:Q", scale=alt.Scale(domain=[min_y, max_y])),
        tooltip=["value"],
        color="variable:N",
        opacity=alt.condition(
            alt.datum.variable == "value", alt.value(1), alt.value(0.3)
        ),
    )
    band = (
        alt.Chart(trend_data[["date", "lower", "upper"]])
        .mark_area(opacity=0.15)
        .encode(
            x=alt.X("date:T"),
            y=alt.Y("lower"),
            y2=alt.Y2("upper"),
        )
    )
    anom_points = (
        alt.Chart(trend_data[["date", "anom_val"]])
        .mark_point(size=50)
        .encode(
            x=alt.X("date:T"),
            y=alt.Y("anom_val"),
            color=alt.value("red"),
            tooltip=["anom_val"],
        )
    )
    chart = value_line + anom_points
    charts = [chart]
    if sst is not None:
        try:
            model = banpei.SST(w=sst)
            results = model.detect(trend_data.value.values)
            d = pd.DataFrame({"date": trend_data["date"], "sst": results})
            sst = (
                alt.Chart(d)
                .mark_line()
                .encode(x="date:T", y="sst:Q")
                .properties(height=100, width=800)
            )
            charts.append(sst)
        except IndexError:
            # make this a logger debug instead
            pass
        except ValueError:
            # same here
            warnings.warn("sst is set to too high a number")
    if add_diff is not None:
        # should this be put in the database?
        # could be useful in a number of different ways, like to detect where biggest changes are occurring
        shift = add_diff.get("shift", 1)
        compute_anomaly = add_diff.get("compute_anomaly", False)
        trend_data["value_diff"] = trend_data.value.diff()
        # trend_data["value_perc_change"] = trend_data.value.pct_change()
        d = pd.DataFrame(
            {"date": trend_data["date"], "value_diff": trend_data["value_diff"]}
        )
        value_diff = (
            alt.Chart(d)
            .mark_line(point=point)
            .encode(x="date:T", y="value_diff:Q", tooltip=["value_diff", "date"])
            .properties(height=100, width=800)
        )
        charts.append(value_diff)
    final_chart = alt.vconcat(*charts).resolve_axis(y="shared")
    if display_notebook:
        final_chart.display()
    else:
        return final_chart


def trend_bar_lateset(data, diff=False, variables=None, axis="metric_id", domain=None):
    if variables is not None:
        data = data[data.metric_id.isin(variables)]
    data.value = data.value.astype(float)
    data = data.sort_values([axis, "date"])
    if diff:
        data["latest_value"] = data.groupby(axis).value.diff()
    else:
        data["latest_value"] = data.value
    data = data.groupby(axis).nth(-1).reset_index()
    batch_name = data.batch_name.iloc[0]
    data = data[["date", axis, "latest_value", "run_name"]]
    all_domains = []
    for domain, group in data.groupby("run_name"):
        bars = (
            alt.Chart(group)
            .mark_bar()
            .encode(
                y=alt.Y(f"{axis}:N", axis=alt.Axis(title=None)),
                x=alt.X(
                    "latest_value:Q",
                    axis=alt.Axis(title="Value Difference" if diff else "Latest Value"),
                ),
                tooltip=["latest_value:Q"],
            )
            .properties(width=700, title=f"{domain}: Total count for {batch_name}")
        )
        chart = bars
        all_domains.append(chart)
    final_chart = (
        alt.vconcat(*all_domains)
        .configure_axis(labelLimit=400)
        .resolve_scale(x="shared")
    )
    return final_chart


def trend_summary(data, variables, axis="metric_id"):
    if variables is not None:
        data = data[data.metric_id.isin(variables)]
    data.value = data.value.astype(float)
    data = data.sort_values(["metric_id", "date"])
    data["value_change"] = data.groupby("metric_id").value.diff()
    data["percentage_change"] = data.groupby("metric_id").value.pct_change()
    data = data.melt(
        id_vars=[axis, "date"],
        value_vars=["percentage_change", "value_change"],
    )
    trends = (
        alt.Chart(data)
        .mark_line(point=True)
        .encode(
            x="date:T", y=alt.Y("value:Q"), color=axis, tooltip=["value", "date", axis]
        )
        .properties(width=800, height=200)
    )
    trends = trends.facet(
        row=alt.Row("variable:N", title=None), title="Summary"
    ).resolve_scale(y="independent")
    trends = trends.configure_title(dx=450).configure_axis(labelLimit=400)
    return trends
