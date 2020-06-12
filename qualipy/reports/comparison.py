import os
import json

import jinja2

from qualipy.reports.base import BaseJinjaView
from qualipy.project import Project
from qualipy.util import get_project_data

from qualipy.visualization.comparison import (
    plot_diffs_altair,
    bar_chart_comparison_altair,
    value_count_comparison_altair,
)


class ComparisonReport(BaseJinjaView):
    def __init__(
        self,
        config_dir,
        comparison_name=None,
        time_zone="US/Eastern",
        custom_styles_directory=None,
    ):
        super(ComparisonReport, self).__init__(custom_styles_directory)
        self.config_dir = config_dir
        self.comparison_name = comparison_name
        with open(os.path.join(config_dir, "config.json"), "rb") as config_file:
            config = json.load(config_file)
        self.comparison_config = config["COMPARISONS"][comparison_name]

        project_keys = sorted(
            [i for i in self.comparison_config.keys() if "project" in i]
        )
        self.data = []
        for project_key in project_keys:
            project_name = self.comparison_config[project_key]
            project = Project(project_name=project_name, config_dir=self.config_dir)
            self.data.append(
                get_project_data(project, time_zone, latest_insert_only=True)
            )

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
