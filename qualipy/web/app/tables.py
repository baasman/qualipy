from flask_table import Table, Col


class AnomalyTable(Table):

    project = Col("project_name")
    column_name = Col("column_name")
    date = Col("date")
    metric = Col("metric")
    arguments = Col("arguments")
    value = Col("value")
    batch_name = Col("batch_name")


class Anomaly(object):
    def __init__(
        self, project, column_name, date, metric, arguments, value, batch_name
    ):
        self.project = project
        self.column_name = column_name
        self.date = date
        self.metric = metric
        self.arguments = arguments
        self.value = value
        self.batch_name = batch_name
