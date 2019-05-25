def find_anomalies_by_std(new_data, hist_data, name, metric, std_away):
    hist_data = hist_data[(hist_data['_name'] == name) &
                               (hist_data['_metric'] == metric)].iloc[:-1]
    mean_of_metric = hist_data.value.mean()
    std = hist_data.value.std()
    new_data_metric = new_data[(new_data['_name'] == name) &
                          (new_data['_metric'] == metric)].value.iloc[0]
    if abs(new_data_metric) > (mean_of_metric + std_away * std):
        return True, new_data_metric
    return False, None
