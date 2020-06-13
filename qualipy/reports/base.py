from jinja2 import (
    ChoiceLoader,
    Environment,
    FileSystemLoader,
    select_autoescape,
)
import altair as alt
import numpy as np
import pandas as pd

from qualipy.anomaly.anomaly import _run_anomaly


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
    ):
        anomaly_data["keep"] = np.where(anomaly_data.type == "boolean", True, False)
        if num_severity_level is not None:
            anomaly_data.keep = np.where(
                (
                    (anomaly_data.severity.notnull())
                    & (
                        (anomaly_data.type == "numerical")
                        | (anomaly_data.metric == "perc_missing")
                        | (anomaly_data["column_name"].isin(["rows", "columns"]))
                    )
                    & (anomaly_data.severity.astype(float).abs() > num_severity_level)
                ),
                True,
                anomaly_data.keep,
            )

        if cat_severity_level is not None:
            anomaly_data.keep = np.where(
                (
                    (anomaly_data.severity.notnull())
                    & ((anomaly_data.type == "categorical"))
                    & (anomaly_data.severity.astype(float).abs() > cat_severity_level)
                ),
                True,
                anomaly_data.keep,
            )
        anomaly_data = anomaly_data[anomaly_data.keep == True].drop("keep", axis=1)

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
        # TODO: FIX THIS
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
