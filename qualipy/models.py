import pandas as pd
import numpy as np

import os
import datetime
import json
import pickle

from qualipy.backends._pandas.generate import GeneratorPandas
from qualipy.backends._pandas.metrics import value_counts, heatmap, is_unique, percentage_missing
from qualipy.database import create_table, get_table, create_alert_table
from qualipy.util import get_column
from qualipy.anomaly_detection import find_anomalies_by_std


HOME = os.path.expanduser('~')


GENERATORS = {
    'pandas': GeneratorPandas,
}


BUILTIN_VIZ = ['value_counts', 'crosstab', 'correlation_plot']
OVERVIEW_PAGE_COLUMNS = ['rows', 'columns', 'index']
OVERVIEW_PAGE_METRICS_DEFAULT = ['perc_missing', 'dtype', 'is_unique']
STANDARD_VIZ = {
    'value_counts': {
        'function': 'value_counts',
        'over_time': True
    },
    'crosstab': {
        'function': 'heatmap',
        'over_time': False
    },
    'correlation': {
        'function': 'heatmap',
        'over_time': False
    }
}


def _create_value(value, metric, name, date):
    return {
        'value': value,
        '_date': date,
        '_name': name,
        '_metric': metric,
        '_standard_viz': np.NaN,
        '_over_time': True,
    }


def _check_for_anomaly(function):
    if isinstance(function, dict):
        check = function.get('check_for_anomalies', False)
        # if check:
        #     return function['function']
        return check
    return False


class DataSet(object):

    def __init__(self, project, engine=None, backend='pandas', reset=False, time_of_run=None):
        self.project = project
        self.alert_table_name = '{}_alerts'.format(project.project_name)
        self.time_of_run = datetime.datetime.now() if time_of_run is None else time_of_run

        self.current_data = None
        self.reset = reset
        self.generator = GENERATORS[backend]()

        if engine is None:
            self.engine = os.path.join(HOME, '.qualipy', 'qualipy.db')
        else:
            self.engine = engine

        self._locate_history_data()
        self._get_alerts()


    def run(self):
        self._generate_metrics()

    def set_dataset(self, df):
        self.current_data = df
        self.schema = {col:
                           {
                               'nullable': info['null'],
                               'unique': info['unique'],
                               'dtype': str(get_column(self.current_data, col).dtype)
                           }
            for col, info in self.project.columns.items()}


    def get_alerts(self, column_name, function_name, std_away=3):

        hist_data = self._locate_history_data()
        hist_data.value = hist_data.value.apply(lambda r: pickle.loads(r))

        nrows = hist_data[(hist_data['_name'] == column_name) &
                          (hist_data['_metric'] == function_name)].shape[0]

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

    def _add_to_project_list(self):
        project_file_path = os.path.join(HOME, '.qualipy', 'projects.json')
        try:
            with open(project_file_path, 'r') as f:
                projects = json.loads(f.read())
        except:
            projects = {}

        if self.project.project_name not in projects or self.reset:
            projects[self.project.project_name] = {
                'columns': list(self.project.columns.keys()),
                'executions': [datetime.datetime.now().strftime('%m/%d/%Y %H:%M')],
                'db': str(self.engine.url),
                'schema': self.schema
            }
        else:
            projects[self.project.project_name]['executions'].append(str(datetime.datetime.now()))
        with open(project_file_path, 'w') as f:
            json.dump(projects, f)


    def _locate_history_data(self):
        with self.engine.connect() as conn:
            exists = conn.execute('select name from sqlite_master '
                                  'where type="table" '
                                  'and name="{}"'.format(self.project.project_name)).fetchone()
            if not exists:
                create_table(self.engine, self.project.project_name)
            if self.reset:
                try:
                    conn.execute('drop table {}'.format(self.project.project_name))
                except:
                    pass
                create_table(self.engine, self.project.project_name)
                conn.execute('delete from {}'.format(self.project.project_name))

            hist_data = get_table(engine=self.engine, table_name=self.project.project_name)
        return hist_data

    def _get_alerts(self):
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

    def _generate_metrics(self):
        measures = []
        anomalies = []
        for col, specs in self.project.columns.items():

            type = specs.get('type', None)
            if type:
                self.current_data = self.generator.set_type(self.current_data, col, type)

            for function_name, function in {**specs['default_functions'], **specs['custom_functions']}.items():
                if function_name in STANDARD_VIZ:
                    standard_viz = STANDARD_VIZ[function_name]['function']
                    over_time = STANDARD_VIZ[function_name]['over_time']
                else:
                    standard_viz = np.NaN
                    over_time = True

                arguments = function.arguments
                other_column = function.other_column
                if other_column is not None:
                    other_columns = {}
                    other_columns[other_column] = self.current_data[other_column]
                else:
                    other_columns = None

                result = self.generator.generate_description(function=function, data=self.current_data, column=col,
                                                             standard_viz=standard_viz, function_name=function_name,
                                                             date=self.time_of_run, other_columns=other_columns,
                                                             over_time=over_time, kwargs=arguments)
                measures.append(result)
                measures = self._get_column_specific_general_info(specs, measures)

        self.current_data = pd.DataFrame(measures)
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
                                                                date=self.time_of_run, over_time=True,
                                                                kwargs={}))
        measures.append(self.generator.generate_description(function=percentage_missing, data=self.current_data, column=col_name,
                                                            standard_viz=np.NaN, function_name='is_unique',
                                                            date=self.time_of_run, over_time=True,
                                                            kwargs={}))
        measures.append(_create_value(str(self.current_data[col_name].dtype), 'dtype',
                                      col_name, self.time_of_run))
        return measures

    def _get_general_info(self, measures):

        # should probably generate unique for everything

        rows, cols = self.current_data.shape
        measures.append(_create_value(rows, 'count', 'rows', self.time_of_run))
        measures.append(_create_value(cols, 'count', 'columns', self.time_of_run))
        return measures

    def _set_type(self, data):
        data['_type'] = 'custom'
        data.loc[data['_metric'].isin(list(STANDARD_VIZ.keys())), '_type'] = 'standard_viz'
        data.loc[data['_name'].isin(OVERVIEW_PAGE_COLUMNS), '_type'] = 'overview'
        data.loc[data['_metric'].isin(OVERVIEW_PAGE_METRICS_DEFAULT), '_type'] = 'overview'
        return data

    def _write(self, measures):
        data = pd.DataFrame(measures)
        data = self._set_type(data)
        self.current_data_measures = data.copy()

        # all values are getting binary data for now, need to think of solution for this
        data.value = data.value.apply(lambda v: pickle.dumps(v))
        data.to_sql(self.project.project_name, self.engine, if_exists='append', index=False)

        self._add_to_project_list()

class DataSet_Deprecated(object):

    def __init__(self, config, backend='pandas', engine=None, reset=False, time_of_run=None):
        self.table_name = config['data_name']
        self.alert_table_name = '{}_alerts'.format(self.table_name)
        self.columns = config['columns']
        self.backend = backend
        self.generator = GENERATORS[backend]()
        self.time_of_run = datetime.datetime.now() if time_of_run is None else time_of_run

        self.current_data = None
        self.reset = reset

        if engine is None:
            self.engine = os.path.join(HOME, '.qualipy', 'qualipy.db')
        else:
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
            projects[self.table_name] = {
                'columns': list(self.columns.keys()),
                'executions': [datetime.datetime.now().strftime('%m/%d/%Y %H:%M')],
                'db': str(self.engine.url),
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
        if metric_name in STANDARD_VIZ:
            standard_viz = STANDARD_VIZ[metric_name]['plot']
            over_time = STANDARD_VIZ[metric_name]['over_time']
        else:
            standard_viz = np.NaN
            over_time= True
        measure = self.generator.generate_description(data=self.current_data, column=column,
                                                      measure=metric_name, standard_viz=standard_viz,
                                                      date=self.time_of_run, over_time=over_time,
                                                      custom_funcs=self.custom_funcs, kwargs=kwargs)
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
        data.loc[data['_metric'].isin(list(STANDARD_VIZ.keys())), '_type'] = 'standard_viz'
        data.loc[data['_name'].isin(OVERVIEW_PAGE_COLUMNS), '_type'] = 'overview'
        data.loc[data['_metric'].isin(OVERVIEW_PAGE_METRICS_DEFAULT), '_type'] = 'overview'
        return data

    def _write(self, measures):
        data = pd.DataFrame(measures)
        data = self._set_type(data)
        self.current_data_measures = data.copy()

        # all values are getting binary data for now, need to think of solution for this
        data.value = data.value.apply(lambda v: pickle.dumps(v))
        data.to_sql(self.table_name, self.engine, if_exists='append', index=False)

        self._add_to_project_list()

