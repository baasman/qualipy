#numeric

def _get_mean(data, column):
    return data[column].mean()


def _get_std(data, column):
    return data[column].std()


def _get_min(data, column):
    return data[column].min()


def _get_quantile(data, column, quantile=.5):
    return data[column].quantile(quantile)


# categorical

def _get_nunique(data, column):
    return data[column].nunique()


def _get_top(data, column):
    return data[column].describe()['top']


def _get_freq(data, column):
    return data[column].describe()['freq']
