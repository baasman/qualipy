import os
import json

import jinja2
import numpy as np

from qualipy.reports.base import BaseJinjaView
from qualipy.project import Project
from qualipy.util import (
    get_anomaly_data,
    get_project_data,
    get_latest_insert_only,
    set_value_type,
)

# from qualipy.anomaly_detection import _run_anomaly
from qualipy.visualization.trends import trend_line_altair
from qualipy.visualization.categorical import (
    value_count_chart_altair,
    barchart_top_categories_altair,
)
from qualipy.visualization.general import (
    missing_by_column_bar_altair,
    row_count_view_altair,
)


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
        self.project = Project(
            config_dir=config_dir, project_name=project_name, re_init=True
        )

        if run_anomaly:
            self._run_anomaly_detection(
                project_name=self.project_name,
                config_dir=self.config_dir,
                retrain_anomaly=retrain_anomaly,
            )

        self.project_data = get_project_data(
            self.project,
            timezone=time_zone,
            latest_insert_only=True,
            floor_datetime=True,
        )
        self.anomaly_data = get_anomaly_data(self.project, timezone=time_zone)
        # limitation - type not part of anomaly table
        if self.anomaly_data.shape[0] > 0:
            self.anomaly_data = self.anomaly_data.merge(
                self.project_data[["metric_id", "type"]].drop_duplicates(),
                on="metric_id",
                how="left",
            )
        if self.project_data.shape[0] == 0:
            raise Exception(f"No data found for project {project_name}")

        if self.project_name not in config:
            raise Exception(
                f"""Must specify {project_name} in your configuration file ({self.config_dir})
                in order to generate an anomly report"""
            )
        self.project_specific_config = config[self.project_name]
        self._get_viz_config(self.project_specific_config)

        self.t1 = t1
        self.t2 = t2
        self.only_show_anomaly = only_show_anomaly
        self.num_severity_level = self.project_specific_config.get("NUM_SEVERITY_LEVEL")
        self.cat_severity_level = self.project_specific_config.get("CAT_SEVERITY_LEVEL")

        self.full_backup_data = self.project_data.copy()
        self.project_data, self.anomaly_data = self._subset_data(
            project_data=self.project_data,
            anomaly_data=self.anomaly_data,
            num_severity_level=self.num_severity_level,
            cat_severity_level=self.cat_severity_level,
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
        self.key_columns = config.get("KEY_COLUMNS")
        self.date_range = config.get("DATE_RANGE", [])
        vis_conf = config.get("VISUALIZATION", {})
        self.trend = vis_conf.get(
            "trend", {"point": False, "sst": 30, "add_diff": None}
        )
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
        if missing_data.shape[0] > 0:
            chart = missing_by_column_bar_altair(missing_data, show_notebook=False)
            plots.append(jinja2.Markup(chart.to_json()))
            perc_missing_plots = self._create_trend_lines(only_missing=True)
            plots.extend(perc_missing_plots)
        return plots

    def _create_row_counts_plot(self):
        row_count_plots = self._create_trend_lines(only_row_counts=True)
        return row_count_plots

    def _create_trend_lines(self, only_missing=False, only_row_counts=False):
        if only_missing:
            num_data = self.project_data[(self.project_data.metric == "perc_missing")]
        elif only_row_counts:
            num_data = self.full_backup_data.copy()
            num_data = num_data[
                (num_data["column_name"].str.contains("rows"))
                & (num_data["metric"] == "count")
            ]
            num_data.value = num_data.value.astype(float)
        else:
            num_data = self.project_data[(self.project_data.type == "numerical")]

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
                            add_diff=self.trend["add_diff"],
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
