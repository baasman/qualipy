import pandas as pd
import pytest


@pytest.fixture()
def dataset():
    data = pd.DataFrame({'integer_col': [1, 2, 3],
                         'float_col': [1.0, 2.0, 3.0],
                         'date_col': ['2018/01/01', '2018/01/02', '2018/01/03']})
    return data
