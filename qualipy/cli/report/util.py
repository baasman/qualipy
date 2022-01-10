import os
import datetime

from qualipy.reports.anomaly import AnomalyReport
from qualipy.reports.batch import BatchReport


def produce_batch_report_cli(
    config_dir, project_name, batch_name, run_name=None, out_file=None
):
    config_dir = os.path.expanduser(config_dir)
    if run_name is None:
        run_name = "0"

    view = BatchReport(
        config_dir=config_dir,
        project_name=project_name,
        batch_name=batch_name,
        run_name=run_name,
    )
    rendered_page = view.render(
        template=f"batch.j2",
        title="Batch Report",
        project_name=project_name,
    )
    if out_file is None:
        time_of_run = datetime.datetime.now().strftime("%Y-%d-%mT%H")
        out_file = os.path.join(
            config_dir,
            "reports",
            "profiler",
            f"{project_name}-{batch_name}-{time_of_run}.html",
        )
    rendered_page.dump(out_file)


def produce_anomaly_report_cli(
    config_dir,
    project_name,
    run_anomaly=False,
    clear_anomaly=False,
    only_show_anomaly=False,
    t1=None,
    t2=None,
    out_file=None,
    run_name=None,
):
    config_dir = os.path.expanduser(config_dir)
    view = AnomalyReport(
        config_dir=config_dir,
        project_name=project_name,
        run_anomaly=run_anomaly,
        retrain_anomaly=clear_anomaly,
        only_show_anomaly=only_show_anomaly,
        t1=t1,
        t2=t2,
        run_name=run_name,
    )
    rendered_page = view.render(
        template=f"anomaly.j2", title="Anomaly Report", project_name=project_name
    )
    if out_file is None:
        time_of_run = datetime.datetime.now().strftime("%Y-%d-%mT%H")
        out_file = os.path.join(
            config_dir, "reports", "anomaly", f"{project_name}-{time_of_run}.html"
        )
    rendered_page.dump(out_file)