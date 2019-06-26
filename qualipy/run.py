import pandas as pd
import numpy as np

import os
import datetime
import pickle

from qualipy.backends._pandas.generate import BackendPandas
from qualipy.backends._pandas.metrics import is_unique, percentage_missing
from qualipy.database import create_table, get_table, create_alert_table
from qualipy.anomaly_detection import find_anomalies_by_std
from qualipy.exceptions import (
    FailException
)
from qualipy.config import (
    OVERVIEW_PAGE_COLUMNS,
    OVERVIEW_PAGE_METRICS_DEFAULT,
    STANDARD_VIZ_STATIC,
    STANDARD_VIZ_DYNAMIC
)


HOME = os.path.expanduser('~')


GENERATORS = {
    'pandas': BackendPandas,
}


def _create_value(value, metric, name, date):
    return {
        'value': value,
        'date': date,
        'column_name': name,
        'metric': metric,
        'standard_viz': np.NaN,
        'is_static': True,
    }

def set_standard_viz_params(function_name, viz_options_static,
                            viz_options_dynamic):
    if function_name in viz_options_static:
        standard_viz = viz_options_static[function_name]['function']
        is_static = True
    elif function_name in viz_options_dynamic:
        standard_viz = viz_options_dynamic[function_name]['function']
        is_static = False
    else:
        standard_viz = np.NaN
        is_static = True
    return standard_viz, is_static



class DataSet(object):

    def __init__(self, project, backend='pandas', reset=False, time_of_run=None):
        self.project = project
        self.alert_table_name = '{}_alerts'.format(project.project_name)
        self.time_of_run = datetime.datetime.now() if time_of_run is None else time_of_run

        self.current_data = None
        self.reset = reset
        self.generator = GENERATORS[backend]()

        self._locate_history_data()
        self._get_alerts()

    def run(self):
        self._generate_metrics()

    def set_dataset(self, df):
        self.current_data = df
        self.schema = self.generator.set_schema(df, self.project.columns)

    def _locate_history_data(self):
        with self.project.engine.connect() as conn:
            exists = conn.execute('select name from sqlite_master '
                                  'where type="table" '
                                  'and name="{}"'.format(self.project.project_name)).fetchone()
            if not exists:
                create_table(self.project.engine, self.project.project_name)
            if self.reset:
                try:
                    conn.execute('drop table {}'.format(self.project.project_name))
                except:
                    pass
                create_table(self.project.engine, self.project.project_name)
                conn.execute('delete from {}'.format(self.project.project_name))

            hist_data = get_table(engine=self.project.engine, table_name=self.project.project_name)
        return hist_data

    def _generate_metrics(self):
        measures = []
        anomalies = []
        for col, specs in self.project.columns.items():

            # enforce type for function
            type = specs['type']
            if type:
                self.current_data = self.generator.set_column_type(self.current_data, col, type)

            for function_name, function in {**specs['default_functions'], **specs['custom_functions']}.items():

                # is this a built in viz, over time?
                standard_viz, is_static = set_standard_viz_params(function_name, STANDARD_VIZ_STATIC,
                                                                  STANDARD_VIZ_DYNAMIC)

                should_fail = function.fail
                arguments = function.arguments
                other_columns = self.generator.get_other_columns(function.other_column,
                                                                 arguments, self.current_data)

                # generate result row
                result = self.generator.generate_description(function=function, data=self.current_data, column=col,
                                                             standard_viz=standard_viz, function_name=function_name,
                                                             date=self.time_of_run, other_columns=other_columns,
                                                             is_static=is_static, kwargs=arguments)

                result['value'] = self.generator.set_return_value_type(result['value'], function.return_format)

                if should_fail and not result['value']:
                    raise FailException("Program halted by function '{}' for variable '{}' with "
                                        "parameter 'fail=True'".format(function_name, col))

                measures.append(result)
                measures = self._get_column_specific_general_info(specs, measures)

        self.current_run = pd.DataFrame(measures)
        for col, specs in self.project.columns.items():
            for function_name, function in {**specs['default_functions'], **specs['custom_functions']}.items():
                if function.anomaly:
                    anomalies.append(self.get_alerts(column_name=col, function_name=function_name,
                                                     std_away=2))

        measures = self._get_general_info(measures)
        self._write(measures)

    def _get_column_specific_general_info(self, specs, measures):
        col_name = specs['name']
        if specs['unique']:
            measures.append(self.generator.generate_description(function=is_unique, data=self.current_data, column=col_name,
                                                                standard_viz=np.NaN, function_name='is_unique',
                                                                date=self.time_of_run, is_static=True,
                                                                kwargs={}))
        measures.append(self.generator.generate_description(function=percentage_missing, data=self.current_data, column=col_name,
                                                            standard_viz=np.NaN, function_name='is_unique',
                                                            date=self.time_of_run, is_static=True,
                                                            kwargs={}))
        measures.append(_create_value(str(self.generator.get_dtype(self.current_data, col_name)), 'dtype',
                                      col_name, self.time_of_run))
        return measures

    def _get_general_info(self, measures):
        rows, cols = self.generator.get_shape(self.current_data)
        measures.append(_create_value(rows, 'count', 'rows', self.time_of_run))
        measures.append(_create_value(cols, 'count', 'columns', self.time_of_run))
        return measures

    def _set_type(self, data):
        data['type'] = 'custom'

        data.loc[data['metric'].isin(list(STANDARD_VIZ_STATIC.keys())), 'type'] = 'standard_viz_static'
        data.loc[data['metric'].isin(list(STANDARD_VIZ_DYNAMIC.keys())), 'type'] = 'standard_viz_dynamic'

        data.loc[data['column_name'].isin(OVERVIEW_PAGE_COLUMNS), 'type'] = 'overview'
        data.loc[data['metric'].isin(OVERVIEW_PAGE_METRICS_DEFAULT), 'type'] = 'overview'
        return data

    def _write(self, measures):
        data = pd.DataFrame(measures)
        data = self._set_type(data)
        self.current_data_measures = data.copy()

        # all values are getting binary data for now, need to think of solution for this
        data.value = data.value.apply(lambda v: pickle.dumps(v))
        data.to_sql(self.project.project_name, self.project.engine, if_exists='append', index=False)

        self.project.add_to_project_list(self.schema)

    def _get_alerts(self):
        with self.project.engine.connect() as conn:
            exists = conn.execute('select name from sqlite_master '
                                  'where type="table" '
                                  'and name="{}"'.format(self.alert_table_name)).fetchone()
            if not exists:
                create_alert_table(self.project.engine, self.alert_table_name)
            if self.reset:
                try:
                    conn.execute('drop table {}'.format(self.alert_table_name))
                except:
                    pass
                create_alert_table(self.project.engine, self.alert_table_name)
                conn.execute('delete from {}'.format(self.alert_table_name))

            self.alert_data = get_table(engine=self.project.engine,
                                        table_name=self.alert_table_name)

    def get_alerts(self, column_name, function_name, std_away=3):

        hist_data = self._locate_history_data()
        hist_data.value = hist_data.value.apply(lambda r: pickle.loads(r))

        nrows = hist_data[(hist_data['column_name'] == column_name) &
                          (hist_data['metric'] == function_name)].shape[0]

        if nrows < 1000:
            print('Not enough batches to accurately determine outliers')
            return

        is_anomaly, value = find_anomalies_by_std(self.current_data, hist_data,
                                                  column_name, column_name,
                                                  std_away)
        if is_anomaly:
            return {
                'column': column_name,
                'std_away': std_away,
                'value': value,
                'date': self.time_of_run,
                'alert_message': 'This value is more than {} standard deviations away'.format(std_away)
            }
        return
