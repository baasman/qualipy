import os
import json

import jinja2
from jinja2.utils import markupsafe
import numpy as np
import pandas as pd

from qualipy.reports.base import (
    BaseJinjaView,
    DEFAULT_FUNCTION_DESCRIPTIONS,
    convert_to_markup,
)
from qualipy.project import Project
from qualipy.util import (
    get_anomaly_data,
    get_project_data,
    get_latest_insert_only,
    set_value_type,
    set_title_name,
)
from qualipy.anomaly.trend_rules import trend_rules

# from qualipy.anomaly_detection import _run_anomaly
from qualipy.reports.visualization.trends import (
    trend_line_altair,
    trend_bar_lateset,
    trend_summary,
)
from qualipy.reports.visualization.categorical import (
    value_count_chart_altair,
    barchart_top_categories_altair,
)
from qualipy.reports.visualization.general import (
    missing_by_column_bar_altair,
    row_count_summary,
)


def list_anomalies(data, metric_id):
    pass


class AnomalyReport(BaseJinjaView):
    def __init__(
        self,
        config,
        project,
        run_name=None,
        run_anomaly=False,
        retrain_anomaly=False,
        only_show_anomaly=False,
        time_zone="US/Eastern",
        t1=None,
        t2=None,
        custom_styles_directory=None,
    ):
        super(AnomalyReport, self).__init__(custom_styles_directory)
        self.config = config
        self.project = project

        if run_anomaly:
            self._run_anomaly_detection(
                project=project,
                config=config,
                retrain_anomaly=retrain_anomaly,
            )

        self.project_data = get_project_data(
            self.project,
            timezone=time_zone,
            latest_insert_only=True,
            floor_datetime=True,
        )
        # if run_name is not None:
        #     # TODO: this should be much better
        #     self.project_data = self.project_data[
        #         self.project_data.run_name.str.contains(run_name)
        #     ]
        self.anomaly_data = get_anomaly_data(self.project, timezone=time_zone)
        if self.anomaly_data.shape[0] > 0:
            # TODO: Do I still need this????
            self.anomaly_data = self.anomaly_data.drop(["run_name"], axis=1)
            self.anomaly_data = self.anomaly_data.merge(
                self.project_data[
                    ["value_id", "original_column_name"]
                ].drop_duplicates(),
                on="value_id",
                how="left",
            )
        if self.project_data.shape[0] == 0:
            raise Exception(f"No data found for project {project.project_name}")

        if project.project_name not in config:
            raise Exception(
                f"""Must specify {project.project_name} in your configuration file ({self.config_dir})
                in order to generate an anomaly report"""
            )
        self.project_specific_config = config[project.project_name]
        self._set_viz_config()
        self._set_project_config()

        self.t1 = t1
        self.t2 = t2
        self.only_show_anomaly = only_show_anomaly

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
            include_0_missing=self.missing.get("include_0", True),
            only_include_runs=self.run_names_to_display,
        )

    def _create_template_vars(self) -> dict:
        anomaly_table = self._show_anomaly_table()
        trend_plots = self._create_trend_lines()
        cat_plots = self._create_categorical_lines()
        missing_plots = self._create_missing_plot()
        row_count_plots = self._create_row_counts_plot()
        js_config = self._get_js_config()

        kwargs = {
            "project_name": self.project_name,
            "title": "Qualipy - Project Report",
            "anomaly_table": anomaly_table,
            "trend_plots": trend_plots,
            "cat_plots": cat_plots,
            "missing_plots": missing_plots,
            "row_plots": row_count_plots,
            "js_config": js_config,
        }
        return kwargs

    def _set_viz_config(self):
        self.anomaly_args = self.project_specific_config.get("ANOMALY_ARGS", {})
        self.anomaly_model = self.project_specific_config.get("ANOMALY_MODEL", "std")
        self.key_columns = self.project_specific_config.get("KEY_COLUMNS")
        self.date_range = self.project_specific_config.get("DATE_RANGE", [])
        vis_conf = self.project_specific_config.get("VISUALIZATION", {})
        self.row_counts = vis_conf.get(
            "row_counts",
            {
                "include_bar_of_latest": {"use": False, "diff": True},
                "include_summary": {"use": False},
            },
        )
        self.trend = vis_conf.get(
            "trend",
            {
                "point": False,
                "sst": 30,
                "add_diff": None,
                "n_steps": 20,
                "include_bar_of_latest": {"use": False},
                "include_summary": {"use": False},
            },
        )
        self.proportion = vis_conf.get("proportion", {})
        self.missing = vis_conf.get(
            "missing", {"include_0": True, "include_bar_of_latest": {"use": False}}
        )
        self.boolean = vis_conf.get("boolean", {})
        self.comparison = vis_conf.get("comparison", [])

    def _get_js_config(self):
        sort_by = self.project_specific_config.get("sort_by", [5, "desc"])
        js_config = {"sort_by": sort_by}
        return js_config

    def _set_project_config(self):
        self.num_severity_level = self.project_specific_config.get("NUM_SEVERITY_LEVEL")
        self.cat_severity_level = self.project_specific_config.get("CAT_SEVERITY_LEVEL")
        self.severity_levels = self.project_specific_config.get("SEVERITY_LEVELS", None)
        self.sort_by = self.project_specific_config.get(
            "SORT_BY", ["date", "decreasing"]
        )
        self.date_format = self.project_specific_config.get("DATE_FORMAT")
        display_conf = self.project_specific_config.get("DISPLAY_NAMES", {})
        if "DEFAULT" in display_conf:
            display_names = display_conf["DEFAULT"]
            display_names = {**display_names, **DEFAULT_FUNCTION_DESCRIPTIONS}
            display_names = pd.DataFrame.from_dict(
                display_names, orient="index"
            ).reset_index()

        if "CUSTOM" in display_conf:
            display_names_custom = display_conf["custom"]
            display_names_custom = pd.DataFrame.from_dict(
                display_names_custom, orient="index"
            ).reset_index()
            display_names = pd.concat([display_names, display_names_custom])
        self.display_names = display_names.rename(columns={"index": "metric"})
        self.run_names_to_display = self.project_specific_config.get(
            "INCLUDE_RUN_NAMES"
        )

    def _show_anomaly_table(self):
        columns = [
            "column_name",
            "domain",
            "metric",
            "error function",
            "arguments",
            "value",
            "severity",
            "date",
        ]
        anom_data = self.anomaly_data.copy()

        anom_data.severity = anom_data.severity.astype(float).round(2)

        if self.severity_levels is not None:
            levels = [(k, v) for k, v in self.severity_levels.items()]
            levels = sorted(levels, key=lambda k: k[1])
            bins = [0] + [i[1] for i in levels]
            labels = [i[0] for i in levels]
            anom_data["severity"] = pd.cut(anom_data.severity, bins, labels=labels)

        if not anom_data.empty:
            anom_data.column_name = anom_data.apply(
                lambda r: '<a href="#'
                + r["metric_id"]
                + '">'
                + r["original_column_name"]
                + "</a>",
                axis=1,
            )
        trend_rules_descriptions = (
            pd.DataFrame.from_dict(trend_rules, orient="index")
            .reset_index()[["index", "display_name", "description"]]
            .rename(columns={"index": "trend_function_name"})
        )
        anom_data = anom_data.merge(
            trend_rules_descriptions, how="left", on="trend_function_name"
        )
        if not anom_data.empty:
            anom_data.trend_function_name = anom_data.apply(
                lambda r: '<a href="#" data-toggle="tooltip" title="'
                + r["description"]
                + '">'
                + r["display_name"]
                + "</a>"
                if r["trend_function_name"] is not None
                else "",
                axis=1,
            )
        anom_data = anom_data.drop(["display_name", "description"], axis=1)
        anom_data = anom_data.merge(self.display_names, how="left", on="metric")
        if not anom_data.empty:
            anom_data.metric = anom_data.apply(
                lambda r: '<a href="#" data-toggle="tooltip" title="'
                + str(r["description"])
                + '">'
                + str(r["display_name"])
                + "</a>",
                axis=1,
            )

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
        if self.date_format is not None:
            anom_data.date = anom_data.date.dt.strftime(self.date_format)
        anom_data = anom_data.rename(
            columns={"run_name": "domain", "trend_function_name": "error function"}
        )

        table = markupsafe.Markup(
            anom_data.to_html(
                index=False,
                columns=columns,
                table_id=self.project_name,
                classes=["table table-striped"],
                escape=False,
                render_links=True,
            )
        )
        return table

    def _create_missing_plot(self):
        plots = []
        missing_data = self.project_data[self.project_data["metric"] == "perc_missing"]
        if (
            self.missing.get("include_bar_of_latest", False)
            and missing_data.shape[0] > 0
        ):
            conf = self.missing["include_bar_of_latest"]
            missing_data = self.project_data[
                (self.project_data["metric"] == "perc_missing")
            ].copy()
            missing_data.value = missing_data.value.astype(float).round(3)
            chart = trend_bar_lateset(
                missing_data,
                diff=conf.get("diff", False),
                variables=conf.get("variables"),
                axis="column_name",
            )
            plots.append(
                convert_to_markup(
                    chart,
                    chart_id="trend_bar_latest_missing",
                    show_by_default=conf.get("show_by_default", True),
                    button_name="Missing data - Latest",
                )
            )
        if missing_data.shape[0] > 0:
            chart = missing_by_column_bar_altair(missing_data, show_notebook=False)
            plots.append(
                convert_to_markup(
                    chart,
                    chart_id="mean_missing_column",
                    button_name="Missing data - Mean",
                )
            )
            perc_missing_plots = self._create_trend_lines(main=False, only_missing=True)
            plots.extend(perc_missing_plots)
        return plots

    def _create_row_counts_plot(self):
        plots = []
        if self.row_counts["include_bar_of_latest"].get("use", False):
            conf = self.row_counts["include_bar_of_latest"]
            row_data = self.project_data[
                (self.project_data["column_name"].str.contains("rows"))
                & (self.project_data["metric"] == "count")
            ]
            chart = trend_bar_lateset(
                row_data,
                diff=conf.get("diff", False),
                variables=conf.get("variables"),
                axis="column_name",
            )
            plots.append(
                convert_to_markup(
                    chart,
                    chart_id="trend_bar_latest_row",
                    show_by_default=conf.get("show_by_default", True),
                    button_name="Row Counts - Latest",
                )
            )
        if self.row_counts["include_summary"].get("use", False):
            conf = self.row_counts["include_summary"]
            chart = row_count_summary(self.project_data)
            plots.append(
                convert_to_markup(
                    chart,
                    chart_id="summary_row",
                    show_by_default=conf.get("show_by_default", True),
                    button_name="Row Counts - Summary",
                )
            )
        row_count_plots = self._create_trend_lines(main=False, only_row_counts=True)
        plots.extend(row_count_plots)
        return plots

    def _create_trend_lines(self, main=True, only_missing=False, only_row_counts=False):
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
            if main and self.trend["include_bar_of_latest"].get("use", False):
                conf = self.trend["include_bar_of_latest"]
                chart = trend_bar_lateset(
                    num_data,
                    diff=conf.get("diff", False),
                    variables=conf.get("variables"),
                )
                # plots.append(jinja2.Markup(chart.to_json()))
                plots.append(
                    convert_to_markup(
                        chart,
                        chart_id="bar_of_latest_trend",
                        show_by_default=conf.get("show_by_default", True),
                        button_name="Trends - Latest",
                    )
                )
            if main and self.trend["include_summary"].get("use", False):
                conf = self.trend["include_summary"]
                chart = trend_summary(
                    num_data,
                    variables=conf.get("variables"),
                )
                plots.append(
                    convert_to_markup(
                        chart,
                        chart_id="summary_trend",
                        show_by_default=conf.get("show_by_default", True),
                        button_name="Trends - Summary",
                    )
                )
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
                            point=self.trend.get("point", True),
                            sst=self.trend.get("sst", None),
                            display_notebook=False,
                            add_diff=self.trend.get("add_diff", None),
                            n_steps=self.trend.get("n_steps", 20),
                        )
                        button_name = set_title_name(trend_data)
                        plots.append(
                            convert_to_markup(
                                chart,
                                chart_id=trend_data.metric_id.iloc[0],
                                show_by_default=True,
                                button_name=button_name,
                            )
                        )
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
                    button_name = set_title_name(var_met_data)
                    if vchart is not None:
                        plots.append(
                            convert_to_markup(
                                chart=vchart,
                                chart_id=var_met_data.metric_id.iloc[0] + "Trends",
                                show_by_default=True,
                                button_name=button_name + "-Trends",
                            )
                        )
                    if bchart is not None:
                        plots.append(
                            convert_to_markup(
                                chart=bchart,
                                chart_id=var_met_data.metric_id.iloc[0] + "Bar",
                                show_by_default=True,
                                button_name=button_name + "-Bar",
                            )
                        )
        return plots
