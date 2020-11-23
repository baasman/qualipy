import os
import json
from functools import reduce

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
        # TODO: make sure it works if column names have spaces or start with numbers
        # TODO: make it possible to filter out run names
        # confirm project dir structure is correct if it exists
        super(BatchReport, self).__init__(custom_styles_directory)
        self.config_dir = os.path.expanduser(config_dir)
        self.project_name = project_name
        self.batch_name = batch_name
        self.run_name = run_name
        with open(os.path.join(self.config_dir, "config.json"), "rb") as config_file:
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

        if isinstance(run_name, list):
            self.batch_data = self.combine_profile_reports(run_name)
        else:
            with open(
                os.path.join(
                    self.config_dir, "profile_data", f"{batch_name}-{run_name}.json"
                ),
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
        if isinstance(self.batch_data["num_corr"], dict):
            corr_data = {
                k: pd.DataFrame(v) for k, v in self.batch_data["num_corr"].items()
            }
        else:
            corr_data = {self.run_name: pd.DataFrame(self.batch_data["num_corr"])}
        all_corrs = []
        for run_name, corr in corr_data.items():
            if corr.shape[0] > 0:
                corr.Correlation = corr.Correlation.round(3)
                strongest_only = True if corr["Variable 1"].nunique() > 20 else False
                plot = plot_correlation(
                    corr,
                    title=f"{run_name} - Correlation (Pearson) between numerical variables",
                    strongest_only=strongest_only,
                )
                corr_plot = convert_to_markup(
                    plot,
                    chart_id=f"{run_name}_num_correlation",
                    show_by_default=True,
                    button_name="Numerical Correlation",
                )
                all_corrs.append(corr_plot)
        return all_corrs

    def _create_categorical_corr_plot(self):
        if isinstance(self.batch_data["cat_corr"], dict):
            corr_data = {
                k: pd.DataFrame(v) for k, v in self.batch_data["cat_corr"].items()
            }
        else:
            corr_data = {self.run_name: pd.DataFrame(self.batch_data["cat_corr"])}
        all_corrs = []
        for run_name, corr in corr_data.items():
            if corr.shape[0] > 0:
                corr.Correlation = corr.Correlation.round(3)
                strongest_only = True if corr["Variable 1"].nunique() > 20 else False
                plot = plot_correlation(
                    corr,
                    title=f"{run_name} - Correlation (Cramer's V) between categorical variables",
                    strongest_only=strongest_only,
                )
                corr_plot = convert_to_markup(
                    plot,
                    chart_id=f"{run_name}_cat_correlation",
                    show_by_default=True,
                    button_name="Categorical Correlation",
                )
                all_corrs.append(corr_plot)
        return all_corrs

    def _create_missing_info(self):
        missing_counts = self.batch_data["missing"]["missing_counts"]
        missing_bar_plot = convert_to_markup(
            barchart_from_dict(missing_counts, x_limits=[0, 1]),
            chart_id="missing_bar_plot",
        )
        if isinstance(self.batch_data["missing"]["missing_correlation"], dict):
            corr_data = {
                k: pd.DataFrame(v)
                for k, v in self.batch_data["missing"]["missing_correlation"].items()
            }
        else:
            corr_data = {
                self.run_name: pd.DataFrame(
                    self.batch_data["missing"]["missing_correlation"]
                )
            }
        all_corrs = []
        for run_name, corr in corr_data.items():
            if corr.shape[0] > 0:
                strongest_only = True if corr["Variable 1"].nunique() > 20 else False
                corr.Correlation = corr.Correlation.round(3)
                corr_plot = convert_to_markup(
                    plot_correlation(
                        corr,
                        strongest_only=strongest_only,
                        title=f"{run_name} - Correlation between missingness",
                    ),
                    chart_id=f"{run_name}correlation_plot",
                )
                all_corrs.append(corr_plot)
        return {
            "missing_bar_plot": missing_bar_plot,
            "missing_corr_plot": all_corrs,
        }

    def combine_profile_reports(self, run_name: list) -> dict:
        all_runs = []
        for name in run_name:
            with open(
                os.path.join(
                    self.config_dir, "profile_data", f"{self.batch_name}-{name}.json"
                ),
                "r",
            ) as f:
                batch_data = json.load(f)
            reconstructed = {}
            for key, item in batch_data.items():
                if item is None:
                    reconstructed[key] = item
                elif key in ["cat_info", "numerical_info", "date_info"]:
                    item = {f"{k}_{name}": v for k, v in item.items()}
                    reconstructed[key] = item
                elif key in ["head", "duplicates"]:
                    reconstructed[key] = item
                elif key in ["num_corr", "cat_corr"]:
                    reconstructed[key] = {name: item}
                elif key in ["missing"]:
                    item["missing_counts"] = {
                        f"{k}_{name}": v for k, v in item["missing_counts"].items()
                    }
                    item["missing_correlation"] = {name: item["missing_correlation"]}
                    reconstructed[key] = item
            all_runs.append(reconstructed)

        total_reconstructed = {}

        # numerical_info
        num_info = [i["numerical_info"] for i in all_runs]
        num_info = reduce(lambda x, y: {**x, **y}, num_info)
        total_reconstructed["numerical_info"] = num_info

        # catergorical info
        cat_info = [i["cat_info"] for i in all_runs]
        cat_info = reduce(lambda x, y: {**x, **y}, cat_info)
        total_reconstructed["cat_info"] = cat_info

        # date info
        date_info = [i["date_info"] for i in all_runs]
        date_info = reduce(lambda x, y: {**x, **y}, date_info)
        total_reconstructed["date_info"] = date_info

        # head
        total_reconstructed["head"] = all_runs[0]["head"]

        # duplicates
        dups = [i["duplicates"] for i in all_runs]
        heads_of_dups = [i["head_of_dups"] for i in dups]
        heads_of_dups = [item for sublist in heads_of_dups for item in sublist]
        number_of_duplicates = sum([i["number_of_duplicates"] for i in dups])
        percentage_of_duplicates = sum(
            [i["percentage_of_duplicates"] for i in dups]
        ) / len(dups)
        total_reconstructed["duplicates"] = {
            "head_of_dups": heads_of_dups,
            "number_of_duplicates": number_of_duplicates,
            "percentage_of_duplicates": percentage_of_duplicates,
        }

        # num corr
        num_corr = [i["num_corr"] for i in all_runs if i["num_corr"] is not None]
        if len(num_corr) > 0:
            num_corr = reduce(lambda x, y: {**x, **y}, num_corr)
        else:
            num_corr = {}
        total_reconstructed["num_corr"] = num_corr

        # cat corr
        cat_corr = [i["cat_corr"] for i in all_runs if i["cat_corr"] is not None]
        if len(cat_corr) > 0:
            cat_corr = reduce(lambda x, y: {**x, **y}, cat_corr)
        else:
            cat_corr = {}
        total_reconstructed["cat_corr"] = cat_corr

        # missing
        missing = [i["missing"] for i in all_runs]
        miss_corr = [i["missing_correlation"] for i in missing]
        if len(miss_corr) > 0:
            miss_corr = reduce(lambda x, y: {**x, **y}, miss_corr)
        else:
            miss_corr = {}
        miss_counts = [i["missing_counts"] for i in missing]
        miss_counts = reduce(lambda x, y: {**x, **y}, miss_counts)
        total_reconstructed["missing"] = {
            "missing_correlation": miss_corr,
            "missing_counts": miss_counts,
        }

        return total_reconstructed
