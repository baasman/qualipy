from qualipy.backends.pandas_backend.generator import BackendPandas

import pytest


@pytest.fixture(scope='module')
def generator():
    gen = BackendPandas()
    return gen


def test_get_shape(dataset, generator):
    rows, cols = generator.get_shape(dataset)
    assert rows == 3
    assert cols == 3


def test_get_dtype(dataset, generator):
    dtype = generator.get_dtype(dataset, 'integer_col')
    assert str(dtype) == 'int64'
