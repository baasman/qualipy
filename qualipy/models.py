import pandas as pd

import os
import random
import datetime
import threading
import json

from qualipy.metrics import _generate_descriptions


iris = {
    'data_name': 'iris',
    'columns': {
        'petal.length': {
            'type': 'float',
            'metrics': ['mean', 'std']
        },
        'petal.width': {
            'type': 'float',
            'metrics': ['mean']
        },
        'variety': {
            'type': 'string',
            'metrics': ['nunique']
        }
    }
}

dtypes = {
    'float': float,
    'int': int,
    'string': str
}

HOME = os.path.expanduser('~')


class DataSet(object):

    def __init__(self, config, reset=False):
        self.table_name = config['data_name']
        self.columns = config['columns']

        self.current_data = None
        self.reset = reset

        self._set_file_name()
        self._add_to_project_list()

    def run(self):
        self.thread = threading.Thread(target=self._generate_metrics)
        self.thread.start()

    def read_pandas(self, df):
        self.current_data = df

    def read_csv(self, file_path, **kwargs):
        self.current_data = pd.read_csv(file_path, **kwargs)

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
            projects[self.table_name] = {}
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
            self.current_data[col] = self.current_data[col].astype(dtypes[metrics['type']])
            if metrics['type'] in ['float', 'int']:
                for metric in metrics['metrics']:
                    num_measure = _generate_descriptions(self.current_data[col], metric)
                    num_measure['_name'] = col
                    num_measure['_date'] = datetime.datetime.now()
                    num_measures.append(num_measure)
            elif metrics['type'] in ['string']:
                for metric in metrics['metrics']:
                    cat_measure = _generate_descriptions(self.current_data[col], metric)
                    cat_measure['_name'] = col
                    cat_measure['_date'] = datetime.datetime.now()
                    cat_measures.append(cat_measure)
        self._write(num_measures, cat_measures)

    def _write(self, num_measures, cat_measures):
        num_data = pd.DataFrame(num_measures)
        cat_data = pd.DataFrame(cat_measures)

        self.metrics = self._get_history_metrics()
        pd.concat([self.hist_num_data, num_data], sort=True).to_csv(self.num_name, index=False)
        pd.concat([self.hist_cat_data, cat_data], sort=True).to_csv(self.cat_name, index=False)


if __name__ == '__main__':
    for _ in range(20):
        data = pd.read_csv('https://gist.githubusercontent.com/netj/8836201/raw/6f9306ad21398ea43cba4f7d537619d0e07d5ae3/iris.csv')
        data['petal.length'] += random.randint(0, 5)
        data['petal.width'] += random.randint(0, 5)
        if _ == 0:
            ds = DataSet(iris, reset=True)
        else:
            ds = DataSet(iris, reset=False)
        ds.read_pandas(data)
        ds.run()
