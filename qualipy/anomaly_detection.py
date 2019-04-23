import numpy as np


def find_anomalies_by_std(new_data, hist_data, name, metric, std_away):
    hist_data = hist_data[(hist_data['_name'] == name) &
                               (hist_data['_metric'] == metric)]
    mean_of_metric = hist_data.value.mean()
    std = hist_data.value.std()
    new_data_metric = new_data[(new_data['_name'] == name) &
                          (new_data['_metric'] == metric)].value.iloc[0]
    if abs(mean_of_metric) > (std_away * std):
        return True
    return False


