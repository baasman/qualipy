import pandas as pd

import os
import datetime
import threading
import json
import pickle

from qualipy.backends._pandas.generate import GeneratorPandas
from qualipy.backends._spark.generate import GeneratorSpark
from qualipy.database import create_table, get_table
from qualipy.util import get_column


HOME = os.path.expanduser('~')


GENERATORS = {
    'pandas': GeneratorPandas,
    'spark': GeneratorSpark
}


BUILTIN_VIZ = ['value_counts']
OVERVIEW = ['rows', 'columns', 'index']
GENERAL_FUNCTIONS = ['perc_missing', 'dtype']


def _create_value(value, metric, name, date):
    return {
        'value': value,
        '_date': date,
        '_name': name,
        '_metric': metric
    }


class DataSet(object):

    def __init__(self, config, backend='pandas', engine=None, reset=False, time_of_run=None):
        self.table_name = config['data_name']
        self.columns = config['columns']
        self.backend = backend
        self.generator = GENERATORS[backend]()

        output = config.get('output')
        if output is not None:
            self.out_type = output['type']
        else:
            self.out_type = 'file'

        self.current_data = None
        self.reset = reset
        self.time_of_run = time_of_run

        self.engine = engine

        self._set_custom_funcs(config)
        self._locate_history_data()


    def run(self):
        # self.thread = threading.Thread(target=self._generate_metrics)
        # self.thread.start()
        self._generate_metrics()

    def set_dataset(self, df):
        self.current_data = df
        self.nullables = {col: info.get('null', False) for col, info in self.columns.items()}
        self.unique = {col: info.get('unique', False) for col, info in self.columns.items()}
        self.only_unique = {k: v for k, v in self.unique.items() if v}
        self.dtypes = df.dtypes
        self.dtypes = self.dtypes.append(pd.Series(df.index.dtype, index=['index']))
        self.schema = {col: {'dtype': str(get_column(self.current_data, col).dtype),
                             'nullable': self.nullables[col],
                             'unique': self.unique[col]}
                       for col in self.columns}

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
        if self.out_type == 'file':
            self.history_name = os.path.join(HOME, '.qualipy/data', '{}.csv'.format(self.table_name))
            if not os.path.isfile(self.history_name) or self.reset:
                pd.DataFrame(columns=['_name', '_date', '_metric', 'value']).to_csv(self.history_name, index=False)
            self.hist_data = pd.read_csv(self.history_name)

        elif self.out_type == 'db':
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

                self.hist_data = get_table(engine=self.engine, table_name=self.table_name)

    def _generate_measure(self, metric, column):
        if isinstance(metric, dict):
            metric_name = metric['function']
            kwargs = metric['parameters']
        else:
            metric_name = metric
            kwargs = {}
        measure = self.generator.generate_description(self.current_data, column, metric_name,
                                                      self.custom_funcs, kwargs)
        measure['_name'] = column
        measure['_date'] = datetime.datetime.now() if self.time_of_run is None else self.time_of_run
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

    def _write(self, measures):
        data = pd.DataFrame(measures)
        data['_type'] = 'custom'
        data.loc[data['_metric'].isin(BUILTIN_VIZ), '_type'] = 'value_count'
        data.loc[data['_name'].isin(OVERVIEW), '_type'] = 'overview'
        data.loc[data['_metric'].isin(GENERAL_FUNCTIONS), '_type'] = 'overview'
        if self.out_type == 'file':
            data.to_csv(self.history_name, index=False, mode='a')
        elif self.out_type == 'db':
            data.value = data.value.apply(lambda v: pickle.dumps(v))
            data.to_sql(self.table_name, self.engine, if_exists='append', index=False)
        else:
            raise ValueError
        self._add_to_project_list()

