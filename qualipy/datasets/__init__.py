import os

import pandas as pd

datetime_cols = {"stocks": ["date"], "flat_data": ["datetime"]}


def load_dataset(dataset_name, backend="pandas"):
    # NOTE: for now, nothing is done with backend
    current_path = os.path.dirname(os.path.abspath(__file__))
    data = pd.read_csv(os.path.join(current_path, f"{dataset_name}.csv"))
    if dataset_name in datetime_cols:
        for col in datetime_cols[dataset_name]:
            data[col] = pd.to_datetime(data[col])
    return data