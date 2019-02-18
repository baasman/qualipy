import pandas as pd

import os
import datetime
import threading
import json

from qualipy.metrics import PANDAS_METRIC_MAP, SPARK_METRIC_MAP
from qualipy.backends._pandas.generate import GeneratorPandas
from qualipy.backends._spark.generate import GeneratorSpark




HOME = os.path.expanduser('~')


METRICS = {
    'pandas': PANDAS_METRIC_MAP,
    'spark': SPARK_METRIC_MAP
}

GENERATORS = {
    'pandas': GeneratorPandas,
    'spark': GeneratorSpark
}


class DataSet(object):

    def __init__(self, config, backend='pandas', reset=False, time_of_run=None):
        self.table_name = config['data_name']
        self.columns = config['columns']
        self.backend = backend
        self.metrics = METRICS[backend]
        self.generator = GENERATORS[backend]()

        self.current_data = None
        self.reset = reset
        self.time_of_run = time_of_run

        self._set_custom_funcs(config)
        self._set_file_name()
        self._add_to_project_list()

    def run(self):
        if self.backend == 'pandas':
            self.thread = threading.Thread(target=self._generate_metrics)
            self.thread.start()
        elif self.backend == 'spark':
            self._generate_metrics()

    def set_dataset(self, df):
        self.current_data = df

    def _set_custom_funcs(self, config):
        custom_funcs = config.get('custom_functions')
        if custom_funcs is not None:
            self.all_custom_funcs = list(custom_funcs.keys())
            self.custom_funcs = custom_funcs
        else:
            self.all_custom_funcs = []
            self.custom_funcs = {}

    def _get_history_metrics(self):
        self.hist_num_data = pd.read_csv(self.num_name)
        self.hist_cat_data = pd.read_csv(self.cat_name)

    def _add_to_project_list(self):
        project_file_path = os.path.join(HOME, '.qualipy', 'projects.json')
        try:
            with open(project_file_path, 'r') as f:
                projects = json.loads(f.read())
        except:
            projects = {}

        if self.table_name not in projects:
            projects[self.table_name] = {
                'columns': list(self.columns.keys())
            }
            with open(project_file_path, 'w') as f:
                json.dump(projects, f)

    def _set_file_name(self):
        self.num_name = os.path.join(HOME, '.qualipy/data', '{}-num.csv'.format(self.table_name))
        if not os.path.isfile(self.num_name) or self.reset:
            pd.DataFrame(columns=['_name', '_date', '_metric', 'value']).to_csv(self.num_name, index=False)
        self.cat_name = os.path.join(HOME, '.qualipy/data', '{}-cat.csv'.format(self.table_name))
        if not os.path.isfile(self.cat_name) or self.reset:
            pd.DataFrame(columns=['_name', '_date', '_metric', 'value']).to_csv(self.cat_name, index=False)

    def _generate_metrics(self):
        num_measures = []
        cat_measures = []
        for col, metrics in self.columns.items():
            type = metrics.get('type', None)
            if type:
                self.current_data = self.generator.set_type(self.current_data, col, type)
            for metric in metrics['metrics']:
                if isinstance(metric, dict):
                    metric_name = metric['function']
                    kwargs = metric['parameters']
                else:
                    metric_name = metric
                    kwargs = {}
                measure = self.generator.generate_description(self.current_data, col, metric_name,
                                                              self.custom_funcs, kwargs)
                measure['_name'] = col
                measure['_date'] = datetime.datetime.now() if self.time_of_run is None else self.time_of_run
                if metrics['type'] in ['float', 'int']:
                    num_measures.append(measure)
                elif metrics['type'] in ['string']:
                    cat_measures.append(measure)
        self._write(num_measures, cat_measures)

    def _write(self, num_measures, cat_measures):
        num_data = pd.DataFrame(num_measures)
        cat_data = pd.DataFrame(cat_measures)

        self.metrics = self._get_history_metrics()
        pd.concat([self.hist_num_data, num_data], sort=True).to_csv(self.num_name, index=False)
        pd.concat([self.hist_cat_data, cat_data], sort=True).to_csv(self.cat_name, index=False)
