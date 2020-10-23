import os
import json

import jinja2
import pandas as pd

from qualipy.reports.base import BaseJinjaView, convert_to_markup
from qualipy.project import Project
from qualipy.util import get_project_data
from qualipy.visualization.batch import (
    plot_correlation,
    numeric_batch_profile,
    histogram_from_custom_bins,
    barchart_top_categories_from_value_counts,
    barchart_from_dict,
    barchart_from_dict_on_dates,
)


class BatchReport(BaseJinjaView):
    def __init__(
        self,
        config_dir,
        project_name,
        batch_name,
        run_name,
        time_zone="US/Eastern",
        custom_styles_directory=None,
    ):
        super(BatchReport, self).__init__(custom_styles_directory)
        self.config_dir = config_dir
        self.project_name = project_name
        self.batch_name = batch_name
        with open(os.path.join(config_dir, "config.json"), "rb") as config_file:
            config = json.load(config_file)

        project = Project(
            project_name=project_name, config_dir=self.config_dir, re_init=True
        )
        try:
            data = get_project_data(project, time_zone, latest_insert_only=True)
            data = data[data.batch_name == batch_name]
            self.data = data
        except AttributeError:
            self.data = pd.DataFrame()

        with open(
            os.path.join(config_dir, "profile_data", f"{batch_name}-{run_name}.json"),
            "r",
        ) as f:
            self.batch_data = json.load(f)

    def _create_template_vars(self) -> dict:
        num_batch_plots = self._create_num_batch_plots()
        head = self._create_head()
        duplicates = self._create_duplicates()
        num_info = self._create_numeric_info()
        cat_info = self._create_categorical_info()
        date_info = self._create_date_info()
        num_corr = self._create_numerical_corr_plot()
        cat_corr = self._create_categorical_corr_plot()
        missing = self._create_missing_info()

        kwargs = {
            "batch_name": self.batch_name,
            "project_name": self.project_name,
            "title": "Qualipy - Batch Report",
            "num_batch_info": num_batch_plots,
            "head": head,
            "duplicates": duplicates,
            "num_info": num_info,
            "cat_info": cat_info,
            "date_info": date_info,
            "num_corr": num_corr,
            "cat_corr": cat_corr,
            "missing": missing,
        }
        return kwargs

    def _create_num_batch_plots(self):
        plots = []
        if self.data.shape[0] > 0:
            num_data = self.data[(self.data.type == "numerical")]
            for metric in num_data.metric.unique():
                df = num_data[num_data.metric == metric]
                plots.append(
                    convert_to_markup(
                        numeric_batch_profile(df, title=metric),
                        chart_id=f"{metric}",
                        show_by_default=True,
                        button_name=metric,
                    )
                )
        return plots

    def _create_head(self):
        head_data = pd.DataFrame(self.batch_data["head"])
        table = jinja2.Markup(
            head_data.to_html(
                index=False,
                columns=head_data.columns,
                table_id="data_head",
                classes=["table table-striped"],
                escape=False,
                render_links=True,
            )
        )
        return table

    def _create_duplicates(self):
        duplicate_data = pd.DataFrame(
            self.batch_data["duplicates"]["head_of_dups"]
        ).head(10)
        table = jinja2.Markup(
            duplicate_data.to_html(
                index=False,
                columns=duplicate_data.columns,
                table_id="data_duplicates",
                classes=["table table-striped"],
                escape=False,
                render_links=True,
            )
        )
        return {
            "head_of_dups": table,
            "number_of_duplicates": self.batch_data["duplicates"][
                "number_of_duplicates"
            ],
            "percentage_of_duplicates": self.batch_data["duplicates"][
                "percentage_of_duplicates"
            ],
        }

    def _create_numeric_info(self):
        info = self.batch_data["numerical_info"]
        info = {
            column: (
                pd.DataFrame(
                    pd.Series(
                        {
                            k: v
                            for k, v in values.items()
                            if k not in ["histogram", "num_facets"]
                        }
                    ).round(2)
                ).rename(columns={0: "value"}),
                values["histogram"],
                values["num_facets"],
            )
            for column, values in info.items()
        }
        num_info = {}
        for column, data in info.items():
            table = data[0].to_html(
                index=True,
                columns=data[0].columns,
                table_id=f"{column}_num_info num_info",
                classes=["table table-striped"],
                escape=False,
            )
            try:
                plot = convert_to_markup(
                    histogram_from_custom_bins(data[1], data[2]),
                    chart_id=f"{column}_histogram",
                )
            except TypeError:
                plot = None
            num_info[column] = (table, plot)

        return num_info

    def _create_categorical_info(self):
        info = self.batch_data["cat_info"]
        info = {
            column: (
                pd.DataFrame(
                    pd.Series(
                        {
                            k: v
                            for k, v in values.items()
                            if k not in ["top_groups", "top_groups_freq", "cat_facets"]
                        }
                    ).round(3)
                ).rename(columns={0: "value"}),
                values["top_groups"],
                values["top_groups_freq"],
                values["cat_facets"],
            )
            for column, values in info.items()
        }
        info = {
            column: (
                data[0].to_html(
                    index=True,
                    columns=data[0].columns,
                    table_id=f"{column}_cat_info cat_info",
                    classes=["table table-striped"],
                    escape=False,
                ),
                convert_to_markup(
                    barchart_top_categories_from_value_counts(
                        data[1], data[2], data[3]
                    ),
                    chart_id=f"{column}_bar_chart",
                ),
            )
            for column, data in info.items()
        }
        return info

    def _create_date_info(self):
        info = self.batch_data["date_info"]
        info = {
            column: (
                pd.DataFrame(
                    pd.Series(
                        {k: v for k, v in values.items() if k not in ["distribution"]}
                    )
                ).rename(columns={0: "value"}),
                values["distribution"],
            )
            for column, values in info.items()
        }
        num_info = {}
        for column, data in info.items():
            table = data[0].to_html(
                index=True,
                columns=data[0].columns,
                table_id=f"{column}_num_info num_info",
                classes=["table table-striped"],
                escape=False,
            )
            try:
                plot = convert_to_markup(
                    barchart_from_dict_on_dates(data[1]),
                    chart_id=f"{column}_histogram",
                )
            except TypeError:
                plot = None
            num_info[column] = (table, plot)

        return num_info

    def _create_numerical_corr_plot(self):
        corr = pd.DataFrame(self.batch_data["num_corr"])
        if corr.shape[0] > 0:
            corr.Correlation = corr.Correlation.round(3)
            strongest_only = True if corr["Variable 1"].nunique() > 20 else False
            plot = plot_correlation(
                corr,
                title="Correlation (Pearson) between numerical variables",
                strongest_only=strongest_only,
            )
            return convert_to_markup(
                plot,
                chart_id="num_correlation",
                show_by_default=True,
                button_name="Numerical Correlation",
            )
        return None

    def _create_categorical_corr_plot(self):
        corr = pd.DataFrame(self.batch_data["cat_corr"])
        if corr.shape[0] > 0:
            corr.Correlation = corr.Correlation.round(3)
            strongest_only = True if corr["Variable 1"].nunique() > 20 else False
            plot = plot_correlation(
                corr,
                title="Correlation (Cramer's V) between categorical variables",
                strongest_only=strongest_only,
            )
            return convert_to_markup(
                plot,
                chart_id="cat_correlation",
                show_by_default=True,
                button_name="Categorical Correlation",
            )
        return None

    def _create_missing_info(self):
        missing_counts = self.batch_data["missing"]["missing_counts"]
        missing_bar_plot = convert_to_markup(
            barchart_from_dict(missing_counts), chart_id="missing_bar_plot"
        )
        missing_corr = pd.DataFrame(self.batch_data["missing"]["missing_correlation"])
        if missing_corr.shape[0] > 0:
            strongest_only = (
                True if missing_corr["Variable 1"].nunique() > 20 else False
            )
            missing_corr.Correlation = missing_corr.Correlation.round(3)
            missing_corr_plot = convert_to_markup(
                plot_correlation(
                    missing_corr,
                    strongest_only=strongest_only,
                    title="Correlation between missingness",
                ),
                chart_id="missing_correlation_plot",
            )
        else:
            missing_corr_plot = None
        return {
            "missing_bar_plot": missing_bar_plot,
            "missing_corr_plot": missing_corr_plot,
        }
