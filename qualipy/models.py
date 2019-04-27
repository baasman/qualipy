import pandas as pd

import os
import datetime
import threading
import json
import pickle

from qualipy.backends._pandas.generate import GeneratorPandas
from qualipy.backends._spark.generate import GeneratorSpark
from qualipy.database import create_table, get_table, create_alert_table
from qualipy.util import get_column
from qualipy.anomaly_detection import find_anomalies_by_std


HOME = os.path.expanduser('~')


GENERATORS = {
    'pandas': GeneratorPandas,
    'spark': GeneratorSpark
}


BUILTIN_VIZ = ['value_counts', 'crosstab', 'correlation_plot']
OVERVIEW = ['rows', 'columns', 'index']
GENERAL_FUNCTIONS = ['perc_missing', 'dtype', 'is_unique']


def _create_value(value, metric, name, date):
    return {
        'value': value,
        '_date': date,
        '_name': name,
        '_metric': metric
    }


def _check_for_anomaly(function):
    if isinstance(function, dict):
        check = function.get('check_for_anomalies', False)
        # if check:
        #     return function['function']
        return check
    return False


class DataSet(object):

    def __init__(self, config, backend='pandas', engine=None, reset=False, time_of_run=None):
        self.table_name = config['data_name']
        self.alert_table_name = '{}_alerts'.format(self.table_name)
        self.columns = config['columns']
        self.backend = backend
        self.generator = GENERATORS[backend]()
        self.time_of_run = datetime.datetime.now() if time_of_run is None else time_of_run

        self.current_data = None
        self.reset = reset

        self.engine = engine

        self._set_custom_funcs(config)
        self._locate_history_data()
        self._get_alerts()


    def run(self):
        self._generate_metrics()

    def set_dataset(self, df):
        self.current_data = df
        self.nullables = {col: info.get('null', False) for col, info in self.columns.items()}
        self.unique = {col: info.get('unique', False) for col, info in self.columns.items()}
        self.only_unique = {k: v for k, v in self.unique.items() if v}

        # props if you can read the next three horribly convoluted lines
        # no way this can ever go wrong
        self.check_anomalies = [{'col': i, 'to_check': [_check_for_anomaly(f) for f
                                                        in self.columns[i]['metrics']]} for i
                                in self.columns]
        self.check_anomalies = [{'col': i['col'], 'to_check':
            [fun['function'] for check, fun in zip(i['to_check'],
                                                 self.columns[i['col']]['metrics']) if check]} for i
                                in self.check_anomalies if any(i['to_check'])]
        self.check_anomalies = {i['col']: i['to_check'] for i in self.check_anomalies}

        self.dtypes = df.dtypes
        self.dtypes = self.dtypes.append(pd.Series(df.index.dtype, index=['index']))
        self.schema = {col: {'dtype': str(get_column(self.current_data, col).dtype),
                             'nullable': self.nullables[col],
                             'unique': self.unique[col]}
                       for col in self.columns}

    def get_alerts(self, std_away=3):
        hist_data = self._locate_history_data()
        hist_data.value = hist_data.value.apply(lambda r: pickle.loads(r))
        anomaly_data = []
        for col, metrics in self.check_anomalies.items():
            for metric in metrics:
                nrows = hist_data[(hist_data['_name'] == col) &
                                  (hist_data['_metric'] == metric)].shape[0]
                if nrows < 10:
                    print('Not enough batches to accurately determine outliers')
                    continue

                is_anomaly, value = find_anomalies_by_std(self.current_data_measures, hist_data, col, metric, std_away)
                if is_anomaly:
                    anomaly_data.append(
                        {
                            'column': col,
                            'std_away': std_away,
                            'value': value,
                            'date': self.time_of_run,
                            'alert_message': 'This value is more than {} standard deviations away'.format(std_away)
                        }
                    )
        anomaly_data = pd.DataFrame(anomaly_data)
        anomaly_data.to_sql(self.alert_table_name, self.engine, if_exists='append', index=False)

    def _set_custom_funcs(self, config):
        custom_funcs = config.get('custom_functions')
        if custom_funcs is not None:
            self.all_custom_funcs = list(custom_funcs.keys())
            self.custom_funcs = custom_funcs
        else:
            self.all_custom_funcs = []
            self.custom_funcs = {}

    def _add_to_project_list(self):
        project_file_path = os.path.join(HOME, '.qualipy', 'projects.json')
        try:
            with open(project_file_path, 'r') as f:
                projects = json.loads(f.read())
        except:
            projects = {}

        if self.table_name not in projects or self.reset:
            if self.engine is not None:
                db = str(self.engine.url)
            else:
                db = None
            projects[self.table_name] = {
                'columns': list(self.columns.keys()),
                'executions': [datetime.datetime.now().strftime('%m/%d/%Y %H:%M')],
                'db': None if db is None else db,
                'schema': self.schema
            }
        else:
            projects[self.table_name]['executions'].append(str(datetime.datetime.now()))
        with open(project_file_path, 'w') as f:
            json.dump(projects, f)


    def _locate_history_data(self):
        with self.engine.connect() as conn:
            exists = conn.execute('select name from sqlite_master '
                                  'where type="table" '
                                  'and name="{}"'.format(self.table_name)).fetchone()
            if not exists:
                create_table(self.engine, self.table_name)
            if self.reset:
                try:
                    conn.execute('drop table {}'.format(self.table_name))
                except:
                    pass
                create_table(self.engine, self.table_name)
                conn.execute('delete from {}'.format(self.table_name))

            hist_data = get_table(engine=self.engine, table_name=self.table_name)
        return hist_data


    def _get_alerts(self):
        # do the same for csv storage
        with self.engine.connect() as conn:
            exists = conn.execute('select name from sqlite_master '
                                  'where type="table" '
                                  'and name="{}"'.format(self.alert_table_name)).fetchone()
            if not exists:
                create_alert_table(self.engine, self.alert_table_name)
            if self.reset:
                try:
                    conn.execute('drop table {}'.format(self.alert_table_name))
                except:
                    pass
                create_alert_table(self.engine, self.alert_table_name)
                conn.execute('delete from {}'.format(self.alert_table_name))

            self.alert_data = get_table(engine=self.engine,
                                        table_name=self.alert_table_name)

    def _generate_measure(self, metric, column):
        if isinstance(metric, dict):
            metric_name = metric['function']
            kwargs = metric.get('parameters', {})
        else:
            metric_name = metric
            kwargs = {}
        measure = self.generator.generate_description(data=self.current_data, column=column, measure=metric_name,
                                                      date=self.time_of_run, custom_funcs=self.custom_funcs, kwargs=kwargs)
        return measure


    def _generate_metrics(self):
        measures = []
        for col, metrics in self.columns.items():
            type = metrics.get('type', None)
            if type:
                self.current_data = self.generator.set_type(self.current_data, col, type)
            for metric in metrics['metrics']:
                measure = self._generate_measure(metric, col)
                measures.append(measure)

        measures = self._get_general_info(measures)
        self._write(measures)

    def _get_general_info(self, measures):

        # should probably generate unique for everything

        rows, cols = self.current_data.shape
        measures.append(_create_value(rows, 'count', 'rows', self.time_of_run))
        measures.append(_create_value(cols, 'count', 'columns', self.time_of_run))
        for col in self.only_unique:
            measures.append(self._generate_measure('is_unique', col))
        for col in self.columns:
            measures.append(self._generate_measure('perc_missing', col))
            measures.append(_create_value(str(self.dtypes[self.dtypes.index == col][0]),
                                          'dtype', col, self.time_of_run))
        return measures

    def _set_type(self, data):
        data['_type'] = 'custom'
        data.loc[data['_metric'].isin(BUILTIN_VIZ), '_type'] = 'built-in-viz'
        data.loc[data['_name'].isin(OVERVIEW), '_type'] = 'overview'
        data.loc[data['_metric'].isin(GENERAL_FUNCTIONS), '_type'] = 'overview'
        return data

    def _write(self, measures):
        data = pd.DataFrame(measures)
        data = self._set_type(data)
        self.current_data_measures = data.copy()

        # all values are getting binary data for now, need to think of solution for this
        data.value = data.value.apply(lambda v: pickle.dumps(v))
        data.to_sql(self.table_name, self.engine, if_exists='append', index=False)

        self._add_to_project_list()

