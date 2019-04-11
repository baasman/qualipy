#numeric

def _get_mean(data, column):
    return data[column].mean()


def _get_std(data, column):
    return data[column].std()


def _get_min(data, column):
    return data[column].min()


def _get_quantile(data, column, quantile=.5):
    return data[column].quantile(quantile)


def _get_number_of_duplicates(data, column):
    return data.shape[0] - data.drop_duplicates().shape[0]


def _get_perc_missing(data, column):
    return data[column].isnull().sum()

def _get_nunique(data, column):
    return data[column].nunique()


def _get_top(data, column):
    return data[column].describe()['top']


def _get_freq(data, column):
    return data[column].describe()['freq']

# non numeric

def _get_value_count(data, column):
    return data[data[column] != 'nan'][column].value_counts().sort_values(ascending=False).head(10).to_dict()

