import os

from jinja2 import (
    ChoiceLoader,
    Environment,
    FileSystemLoader,
    select_autoescape,
    Markup,
)
import altair as alt
import numpy as np
import pandas as pd

from qualipy.anomaly.anomaly import _run_anomaly


DEFAULT_FUNCTION_DESCRIPTIONS = {
    "count": {
        "display_name": "count",
        "description": "A simple count of rows in the batch subset",
    },
    "perc_missing": {
        "display_name": "Percentage Missing",
        "description": "Percentage of values that are NaN",
    },
}


def convert_to_markup(
    chart,
    chart_id,
    show_by_default=True,
    button_name="Hide plot",
    list_of_anomalies=None,
):
    show = "show" if show_by_default else ""
    markup = Markup(chart.to_json())
    chart_id = chart_id.replace(" ", "_")
    if chart_id[0].isnumeric():
        chart_id = "_" + chart_id
    plot = {
        "chart": markup,
        "show_by_default": show,
        "button_name": button_name,
        "chart_id": chart_id,
        "list_of_anomalies": list_of_anomalies,
    }
    return plot


class BaseJinjaView:
    def __init__(self, custom_styles_directory):
        self.custom_styles_directory = custom_styles_directory

    def render(self, template, **kwargs):
        self._set_global_altair_options()
        template_ = self._get_template(template)
        kwargs = self._create_template_vars()
        return template_.stream(**kwargs)

    def _set_global_altair_options(self):
        alt.data_transformers.disable_max_rows()
        alt.themes.enable("quartz")

    def _create_template_vars(self):
        return {}

    def _subset_data(
        self,
        project_data,
        anomaly_data,
        num_severity_level=None,
        cat_severity_level=None,
        t1=None,
        t2=None,
        only_show_anomaly=False,
        key_columns=None,
        include_0_missing=True,
    ):
        if anomaly_data.shape[0] > 0:
            anomaly_data["keep"] = True
            # anomaly_data["keep"] = np.where(anomaly_data.type == "boolean", True, False)
            # anomaly_data["keep"] = np.where(anomaly_data.severity.isnull(), True, False)
            if num_severity_level is not None:
                anomaly_data.keep = np.where(
                    (
                        (anomaly_data.severity.notnull())
                        & (
                            (anomaly_data.type == "numerical")
                            | (anomaly_data.metric == "perc_missing")
                            | (anomaly_data["column_name"].str.contains("rows"))
                        )
                        & (
                            anomaly_data.severity.astype(float).abs()
                            < num_severity_level
                        )
                    ),
                    False,
                    anomaly_data.keep,
                )

            if cat_severity_level is not None:
                anomaly_data.keep = np.where(
                    (
                        (anomaly_data.severity.notnull())
                        & ((anomaly_data.type == "categorical"))
                        & (
                            anomaly_data.severity.astype(float).abs()
                            < cat_severity_level
                        )
                    ),
                    False,
                    anomaly_data.keep,
                )
            anomaly_data = anomaly_data[anomaly_data.keep == True].drop("keep", axis=1)

            if t1 is not None and t2 is not None and anomaly_data.shape[0] > 0:
                t1 = pd.to_datetime(t1)
                t2 = pd.to_datetime(t2)
                anomaly_data = anomaly_data[
                    (anomaly_data.date >= t1) & (anomaly_data.date <= t2)
                ]
        if not include_0_missing:
            missing = project_data[project_data.metric == "perc_missing"]
            metrics_with_missing = missing[
                missing.value.astype(float) > 0
            ].metric_id.unique()
            project_data = project_data[
                (project_data.metric_id.isin(metrics_with_missing))
                | (project_data.metric != "perc_missing")
            ]

        if only_show_anomaly:
            if anomaly_data.shape[0] == 0:
                raise Exception(
                    'You specified "only_show_anomaly" but no anomalies were found'
                )
            project_data = project_data.merge(
                anomaly_data[["column_name", "metric"]],
                on=["column_name", "metric"],
            ).drop_duplicates()
        if key_columns is not None:
            project_data = project_data[project_data.column_name.isin(key_columns)]
        return project_data, anomaly_data

    def _run_anomaly_detection(self, project_name, config_dir, retrain_anomaly):
        _run_anomaly(
            project_name=project_name,
            config_dir=config_dir,
            retrain=retrain_anomaly,
        )

    def _get_template(self, template=None):
        current_path = os.path.dirname(os.path.abspath(__file__))
        templates_loader = FileSystemLoader(
            searchpath=os.path.join(current_path, "templates")
        )
        styles_loader = FileSystemLoader(
            searchpath=os.path.join(current_path, "static")
        )

        loaders = [templates_loader, styles_loader]

        if self.custom_styles_directory:
            loaders.append(FileSystemLoader(self.custom_styles_directory))

        env = Environment(
            loader=ChoiceLoader(loaders),
            autoescape=select_autoescape(["html", "xml"]),
            extensions=["jinja2.ext.do"],
        )

        template = env.get_template(template)
        return template
