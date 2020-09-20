import os
import json

import jinja2

from qualipy.reports.base import BaseJinjaView, convert_to_markup
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
        if "COMPARISONS" not in config:
            raise Exception("No comparisons specified in the configuration")
        self.comparison_config = config["COMPARISONS"][comparison_name]

        project_names = self.comparison_config["projects"]
        self.data = []
        for project_name in project_names:
            project = Project(
                project_name=project_name, config_dir=self.config_dir, re_init=True
            )
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
            id_name = "-".join(comp)
            plots.append(
                convert_to_markup(
                    chart,
                    chart_id=id_name,
                    show_by_default=True,
                    button_name=id_name[:20],
                )
            )
        return plots

    def _create_cat_comparison_plot(self):
        plots = []
        cat_comparisons = self.comparison_config.get("cat_metrics", [])
        for comp in cat_comparisons:
            chart = bar_chart_comparison_altair(self.data, comp, show_notebook=False)
            id_name = "-".join(comp)
            plots.append(
                convert_to_markup(
                    chart,
                    chart_id=id_name + "Bar",
                    show_by_default=True,
                    button_name=id_name[:20] + " - Bar",
                )
            )
            chart = value_count_comparison_altair(self.data, comp, show_notebook=False)
            plots.append(
                convert_to_markup(
                    chart,
                    chart_id=id_name + "Counts",
                    show_by_default=True,
                    button_name=id_name[:20] + " - Counts",
                )
            )
        return plots
