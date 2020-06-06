from qualipy.project import Project
from qualipy.util import (
    get_anomaly_data,
    get_project_data,
    get_latest_insert_only,
    set_value_type,
)
from qualipy.anomaly_detection import _run_anomaly
from qualipy.visualization.trends import trend_line_altair
from qualipy.visualization.categorical import (
    value_count_chart_altair,
    barchart_top_categories_altair,
)
from qualipy.visualization.general import (
    missing_by_column_bar_altair,
    row_count_view_altair,
)
from qualipy.visualization.comparison import (
    plot_diffs_altair,
    bar_chart_comparison_altair,
    value_count_comparison_altair,
)

from jinja2 import (
    ChoiceLoader,
    Environment,
    FileSystemLoader,
    PackageLoader,
    contextfilter,
    select_autoescape,
)
import jinja2
import altair as alt
import numpy as np
import pandas as pd

import os
import json


class BaseJinjaView(object):
    def __init__(self, custom_styles_directory):
        self.custom_styles_directory = custom_styles_directory

    def render(self, template, **kwargs):
        self._set_global_altair_options()
        t = self._get_template(template)
        kwargs = self._create_template_vars()
        return t.stream(**kwargs)

    def _set_global_altair_options(self):
        alt.data_transformers.disable_max_rows()
        alt.themes.enable("quartz")

    def _create_template_vars(self):
        return

    def _subset_data(
        self,
        project_data,
        anomaly_data,
        severity_level=None,
        t1=None,
        t2=None,
        only_show_anomaly=False,
        key_columns=None,
    ):
        if severity_level is not None:
            anomaly_data = anomaly_data[
                (anomaly_data.severity.isnull())
                | (
                    (anomaly_data.severity.notnull())
                    & (anomaly_data.severity.astype(float).abs() > severity_level)
                )
            ]

        if t1 is not None and t2 is not None and anomaly_data.shape[0] > 0:
            t1 = pd.to_datetime(t1)
            t2 = pd.to_datetime(t2)
            anomaly_data = anomaly_data[
                (anomaly_data.date >= t1) & (anomaly_data.date <= t2)
            ]

        if only_show_anomaly:
            project_data = project_data.merge(
                anomaly_data[["column_name", "metric"]], on=["column_name", "metric"],
            ).drop_duplicates()
        if key_columns is not None:
            project_data = project_data[project_data.column_name.isin(key_columns)]
        return project_data, anomaly_data

    def _run_anomaly_detection(
        self, backend_url, project_name, config_dir, retrain_anomaly
    ):
        _run_anomaly(
            backend=backend_url,
            project_name=project_name,
            config_dir=config_dir,
            retrain=retrain_anomaly,
        )

    def _get_template(self, template=None):
        templates_loader = FileSystemLoader(
            searchpath="/home/baasman/Qualipy/qualipy/reports/templates"
        )
        styles_loader = FileSystemLoader(
            searchpath="/home/baasman/Qualipy/qualipy/reports/static"
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


class AnomalyReport(BaseJinjaView):
    def __init__(
        self,
        config_dir,
        project_name,
        run_anomaly=False,
        retrain_anomaly=False,
        only_show_anomaly=False,
        time_zone="US/Eastern",
        t1=None,
        t2=None,
        custom_styles_directory=None,
    ):
        super(AnomalyReport, self).__init__(custom_styles_directory)
        self.config_dir = config_dir
        with open(os.path.join(config_dir, "config.json"), "rb") as cf:
            config = json.load(cf)
        self.project_name = project_name
        self.project = Project(config_dir=config_dir, project_name=project_name)

        if run_anomaly:
            self._run_anomaly_detection(
                backend_url=str(self.project.engine.url),
                project_name=self.project_name,
                config_dir=self.config_dir,
                retrain_anomaly=retrain_anomaly,
            )

        self.project_data = get_project_data(self.project, timezone=time_zone)
        self.anomaly_data = get_anomaly_data(self.project, timezone=time_zone)
        if self.project_data.shape[0] == 0:
            raise Exception(f"No data found for project {project_name}")

        self.project_specific_config = config[self.project_name]
        self._get_viz_config(self.project_specific_config)

        self.t1 = t1
        self.t2 = t2
        self.only_show_anomaly = only_show_anomaly
        self.severity_level = self.project_specific_config.get("severity_level")

        self.full_backup_data = self.project_data.copy()
        self.project_data, self.anomaly_data = self._subset_data(
            project_data=self.project_data,
            anomaly_data=self.anomaly_data,
            severity_level=self.severity_level,
            t1=self.t1,
            t2=self.t2,
            only_show_anomaly=self.only_show_anomaly,
            key_columns=self.key_columns,
        )

    def _create_template_vars(self) -> dict:
        anomaly_table = self._show_anomaly_table()
        trend_plots = self._create_trend_lines()
        cat_plots = self._create_categorical_lines()
        missing_plots = self._create_missing_plot()
        row_count_plots = self._create_row_counts_plot()

        kwargs = {
            "project_name": self.project_name,
            "title": "Qualipy - Project Report",
            "anomaly_table": anomaly_table,
            "trend_plots": trend_plots,
            "cat_plots": cat_plots,
            "missing_plots": missing_plots,
            "row_plots": row_count_plots,
        }
        return kwargs

    def _get_viz_config(self, config):
        self.anomaly_args = config.get("ANOMALY_ARGS", {})
        self.anomaly_model = config.get("ANOMALY_MODEL", "std")
        self.severity_level = config.get("SEVERITY_LEVEL", None)
        self.key_columns = config.get("KEY_COLUMNS")
        self.date_range = config.get("DATE_RANGE", [])
        vis_conf = config.get("VISUALIZATION", {})
        self.trend = vis_conf.get("trend", {"point": False, "sst": 30})
        self.proportion = vis_conf.get("proportion", {})
        self.missing = vis_conf.get("missing", {})
        self.boolean = vis_conf.get("boolean", {})
        self.comparison = vis_conf.get("comparison", [])

    def _show_anomaly_table(self):
        columns = ["column_name", "metric", "arguments", "value", "severity", "date"]
        anom_data = self.anomaly_data.copy()
        anom_data.severity = anom_data.severity.astype(float).round(2)
        anom_data.value = np.where(
            anom_data.value.str.len() > 30,
            anom_data.value.str.slice(0, 30) + "...",
            anom_data.value,
        )
        anom_data.arguments = np.where(
            anom_data.arguments.str.len() > 30,
            anom_data.arguments.str.slice(0, 30) + "...",
            anom_data.arguments,
        )
        table = jinja2.Markup(
            anom_data.to_html(
                index=False,
                columns=columns,
                table_id=self.project_name,
                classes=["table table-striped"],
            )
        )
        return table

    def _create_missing_plot(self):
        plots = []
        missing_data = self.project_data[self.project_data["metric"] == "perc_missing"]
        chart = missing_by_column_bar_altair(missing_data, show_notebook=False)
        plots.append(jinja2.Markup(chart.to_json()))
        return plots

    def _create_row_counts_plot(self):
        plots = []
        data = self.full_backup_data.copy()
        data = get_latest_insert_only(data)
        chart = row_count_view_altair(
            data.copy(),
            anom_data=self.anomaly_data,
            only_anomaly=False,
            show_notebook=False,
        )
        plots.append(jinja2.Markup(chart.to_json()))
        return plots

    def _create_trend_lines(self):
        num_data = self.project_data[
            (self.project_data.type == "numerical")
            | (self.project_data.metric == "perc_missing")
        ]
        num_data = get_latest_insert_only(num_data)
        plots = []
        if num_data.shape[0] > 0:
            columns = num_data.column_name.unique()
            for var in columns:
                var_data = num_data[num_data.column_name == var].copy()
                for metric_id in var_data.metric_id.unique():
                    trend_data = var_data[
                        (var_data.metric_id == metric_id) & (var_data.value.notnull())
                    ].copy()
                    if trend_data.shape[0] > 0:
                        trend_data = set_value_type(trend_data)
                        anom_trend = self.anomaly_data[
                            (self.anomaly_data.column_name == var)
                            & (self.anomaly_data.metric_id == metric_id)
                        ]
                        chart = trend_line_altair(
                            trend_data,
                            var,
                            self.config_dir,
                            self.project_name,
                            anom_trend,
                            point=self.trend["point"],
                            sst=self.trend["sst"],
                            display_notebook=False,
                        )
                        plots.append(jinja2.Markup(chart.to_json()))
        return plots

    def _create_categorical_lines(self):
        plots = []
        cat_data = self.project_data[(self.project_data["type"] == "categorical")]
        cat_data = get_latest_insert_only(cat_data)
        if cat_data.shape[0] > 0:
            cat_data = set_value_type(cat_data)
            columns = cat_data.column_name.unique()
            for var in columns:
                var_data = cat_data[cat_data.column_name == var]
                var_anom = self.anomaly_data[self.anomaly_data.column_name == var]
                metrics = var_data.metric_id.unique()
                for met in metrics:
                    var_met_data = var_data[var_data.metric_id == met]
                    var_anom_met_data = var_anom[var_anom.metric_id == met]
                    vchart = value_count_chart_altair(
                        var_met_data,
                        var,
                        var_anom_met_data,
                        top_n=20,
                        show_notebook=False,
                    )
                    bchart = barchart_top_categories_altair(
                        var_met_data, var, top_n=20, show_notebook=False
                    )
                    plots.append(jinja2.Markup(vchart.to_json()))
                    plots.append(jinja2.Markup(bchart.to_json()))
        return plots


class ComparisonReport(BaseJinjaView):
    def __init__(
        self,
        config_dir,
        comparison_name=None,
        time_zone="US/Eastern",
        t1=None,
        t2=None,
        custom_styles_directory=None,
    ):
        super(ComparisonReport, self).__init__(custom_styles_directory)
        self.config_dir = config_dir
        self.comparison_name = comparison_name
        with open(os.path.join(config_dir, "config.json"), "rb") as cf:
            config = json.load(cf)
        self.comparison_config = config["COMPARISONS"][comparison_name]

        project_keys = sorted(
            [i for i in self.comparison_config.keys() if "project" in i]
        )
        self.data = []
        for project_key in project_keys:
            project_name = self.comparison_config[project_key]
            project = Project(project_name=project_name, config_dir=self.config_dir)
            self.data.append(
                get_project_data(project, "US/Eastern", latest_insert_only=True)
            )

        self.t1 = t1
        self.t2 = t2

    def _create_template_vars(self) -> dict:
        num_plots = self._create_num_comparison_plot()
        cat_plots = self._create_cat_comparison_plot()

        kwargs = {
            "comparison_name": self.comparison_name,
            "title": "Qualipy - Comparison Report",
            "num_plots": num_plots,
            "cat_plots": cat_plots,
        }
        return kwargs

    def _create_num_comparison_plot(self):
        plots = []
        time_freq = self.comparison_config.get("time_freq", "1D")
        num_comparisons = self.comparison_config["num_metrics"]
        for comp in num_comparisons:
            chart = plot_diffs_altair(self.data, comp, time_freq, show_notebook=False)
            plots.append(jinja2.Markup(chart.to_json()))
        return plots

    def _create_cat_comparison_plot(self):
        plots = []
        cat_comparisons = self.comparison_config["cat_metrics"]
        for comp in cat_comparisons:
            chart = bar_chart_comparison_altair(self.data, comp, show_notebook=False)
            plots.append(jinja2.Markup(chart.to_json()))
            chart = value_count_comparison_altair(self.data, comp, show_notebook=False)
            plots.append(jinja2.Markup(chart.to_json()))
        return plots
