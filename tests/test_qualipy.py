from qualipy import DataSet, Project, Column, function, PandasTable
from qualipy.backends.pandas_backend.pandas_types import (
    IntType,
    FloatType,
    DateTimeType,
    NumericType,
)
from qualipy.backends.pandas_backend.functions import (
    mean,
    std,
    percentage_missing,
    number_of_outliers,
    correlation_two_columns,
)
from qualipy.exceptions import InvalidType, NullableError

import pytest
import pandas as pd
import numpy as np

import os
import json
import shutil


@pytest.fixture
def data():
    data = pd.DataFrame(
        {
            "integer_col": [1, 2, 3],
            "float_col": [1.0, 2.0, 3.0],
            "date_col": ["2018/01/01", "2018/01/02", "2018/01/03"],
        }
    )
    data.date_col = pd.to_datetime(data.date_col)
    return data


@pytest.fixture
def project():
    config_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), ".qualipy")
    yield Project(project_name="test", config_dir=config_dir)
    try:
        shutil.rmtree(config_dir)
    except Exception as e:
        print(str(e))


####### project checks ##########


def test_project_get_table(data, project):
    class TestCol(Column):
        column_name = "integer_col"
        column_type = IntType()
        force_type = True

    project.add_column(TestCol())

    ds = DataSet(project=project, batch_name="test")
    ds.set_dataset(data)
    ds.run()

    hist_data = project.get_project_table()
    assert hist_data.shape[0] > 0


def test_project_file(data, project):
    class TestCol(Column):
        column_name = "integer_col"
        column_type = IntType()
        force_type = True

    project.add_column(TestCol())

    ds = DataSet(project=project, batch_name="test")
    ds.set_dataset(data)
    ds.run()

    with open(os.path.join(project.config_dir, "projects.json"), "r") as f:
        project_file = json.loads(f.read())

    assert "test" in project_file

    project.delete_from_project_config()

    with open(os.path.join(project.config_dir, "projects.json"), "r") as f:
        project_file = json.loads(f.read())

    assert "test" not in project_file

    project.add_to_project_list(
        schema={"integer_col": {"unique": False, "dtype": "int32", "nullable": True}}
    )

    with open(os.path.join(project.config_dir, "projects.json"), "r") as f:
        project_file = json.loads(f.read())

    assert "test" in project_file


def test_default_checks_done(data, project):
    class TestCol(Column):
        column_name = "integer_col"
        column_type = IntType()
        force_type = True

    project.add_column(TestCol())

    ds = DataSet(project=project, batch_name="test")
    ds.set_dataset(data)
    ds.run()

    hist_data = project.get_project_table()
    assert (
        hist_data[hist_data.metric == "perc_missing"].value.astype(float).values[0] == 0
    )
    assert (
        hist_data[(hist_data.metric == "count") & (hist_data.column_name == "rows")]
        .value.astype(int)
        .values[0]
        == 3
    )
    assert (
        hist_data[(hist_data.metric == "count") & (hist_data.column_name == "columns")]
        .value.astype(int)
        .values[0]
        == 3
    )
    assert hist_data[hist_data.metric == "dtype"].value.values[0] == "int64"


def test_delete_data(data, project):
    class TestCol(Column):
        column_name = "integer_col"
        column_type = IntType()
        force_type = True

    project.add_column(TestCol())

    ds = DataSet(project=project, batch_name="test")
    ds.set_dataset(data)
    ds.run()

    hist_data = project.get_project_table()
    assert hist_data.shape[0] > 0

    project.delete_data()

    hist_data = project.get_project_table()
    assert hist_data.shape[0] == 0


######## Tables and chunking ######


def test_read_table(data, project):
    class Table(PandasTable):
        columns = ["integer_col", "float_col", "date_col"]
        infer_schema = False
        table_name = "test"
        types = {
            "integer_col": IntType(),
            "float_col": FloatType(),
            "date_col": DateTimeType(),
        }

    project.add_table(Table())

    assert len(project.columns) == 3


def test_read_table_infer_schema(data, project):
    class Table(PandasTable):
        columns = "all"
        infer_schema = True
        table_name = "test"

        def extract_sample_row(self):
            return data

    project.add_table(Table())

    assert len(project.columns) == 3


def test_table_equals_columns(data, project):
    class Table(PandasTable):
        columns = ["integer_col"]
        infer_schema = False
        table_name = "test"
        types = {"integer_col": IntType()}

    project.add_table(Table())

    ds = DataSet(project=project, batch_name="test")
    ds.set_dataset(data)
    ds.run()

    hist_data = project.get_project_table()
    assert hist_data.shape[0] == 7

    project.columns = {}
    project.delete_from_project_config()
    project.delete_data()

    class TestCol(Column):
        column_name = "integer_col"
        column_type = IntType()
        force_type = True
        functions = [mean, std, percentage_missing]

    project.add_column(TestCol())

    ds = DataSet(project=project, batch_name="test")
    ds.set_dataset(data)
    ds.run()

    hist_data = project.get_project_table()
    assert hist_data.shape[0] == 7

    hist_data = project.get_project_table()
    assert hist_data.shape[0] > 0


def test_table_extra_functions(data, project):
    @function(return_format=float)
    def test_fun(data, column):
        return 0

    class Table(PandasTable):
        columns = ["integer_col"]
        infer_schema = False
        table_name = "test"
        types = {"integer_col": IntType()}

        extra_functions = {"integer_col": [{"function": test_fun}]}

    project.add_table(Table())

    ds = DataSet(project=project, batch_name="test")
    ds.set_dataset(data)
    ds.run()

    hist_data = project.get_project_table()
    assert "test_fun" in hist_data.metric.values


######## type checking #########


def test_type_check(data, project):
    class TestCol(Column):
        column_name = "integer_col"
        column_type = IntType()
        force_type = True

    project.add_column(TestCol())

    ds = DataSet(project=project, batch_name="test")
    ds.set_dataset(data)
    ds.run()

    assert 1


def test_type_check_fail(data, project):
    class TestCol(Column):
        column_name = "integer_col"
        column_type = IntType()
        force_type = True

    project.add_column(TestCol())

    data.integer_col = data.integer_col.astype(float)
    ds = DataSet(project=project, batch_name="test")
    ds.set_dataset(data)
    with pytest.raises(InvalidType):
        ds.run()


######## null checking #########


def test_null_check(data, project):
    class TestCol(Column):
        column_name = "integer_col"
        column_type = IntType()
        null = False
        force_null = True

    project.add_column(TestCol())

    ds = DataSet(project=project, batch_name="test")
    ds.set_dataset(data)
    ds.run()

    assert 1


def test_null_check_fail(data, project):
    class TestCol(Column):
        column_name = "float_col"
        column_type = FloatType()
        null = False
        force_null = True

    project.add_column(TestCol())

    data.loc[0, "float_col"] = np.NaN
    ds = DataSet(project=project, batch_name="test")
    ds.set_dataset(data)
    with pytest.raises(NullableError):
        ds.run()


############# functions ##############


def test_function_call_without_arguments(data, project):
    class TestCol(Column):
        column_name = "integer_col"
        column_type = IntType()
        null = False
        force_null = True
        functions = [mean]

    project.add_column(TestCol())

    ds = DataSet(project=project, batch_name="test")
    ds.set_dataset(data)
    ds.run()

    hist_data = project.get_project_table()
    assert hist_data[hist_data.metric == "mean"].value.astype(float).values[0] == 2.0


def test_function_call_without_arguments_dict_style(data, project):
    class TestCol(Column):
        column_name = "integer_col"
        column_type = IntType()
        null = False
        force_null = True
        functions = [{"function": mean}]

    project.add_column(TestCol())

    ds = DataSet(project=project, batch_name="test")
    ds.set_dataset(data)
    ds.run()

    hist_data = project.get_project_table()
    assert hist_data[hist_data.metric == "mean"].value.astype(float).values[0] == 2.0


def test_function_call_with_arguments(data, project):
    class TestCol(Column):
        column_name = "integer_col"
        column_type = IntType()
        null = False
        force_null = True
        functions = [{"function": number_of_outliers, "parameters": {"std_away": 2}}]

    project.add_column(TestCol())

    ds = DataSet(project=project, batch_name="test")
    ds.set_dataset(data)
    ds.run()

    hist_data = project.get_project_table()
    assert (
        hist_data[hist_data.metric == "number_of_outliers"].value.astype(int).values[0]
        == 0
    )


###### multiple columns #############


def test_multiple_columns(data, project):
    class TestCol(Column):
        column_name = ["integer_col", "float_col"]
        column_type = NumericType()
        null = False
        force_null = True

    project.add_column(TestCol())

    ds = DataSet(project=project, batch_name="test")
    ds.set_dataset(data)
    ds.run()

    hist_data = project.get_project_table()
    assert "integer_col" in hist_data.column_name.values
    assert "float_col" in hist_data.column_name.values


###### test other columns #############


def test_other_column(data, project):
    class TestCol(Column):
        column_name = ["integer_col"]
        column_type = IntType()
        null = False
        force_null = True
        functions = [
            {
                "function": correlation_two_columns,
                "parameters": {"column_two": "float_col"},
            }
        ]

    project.add_column(TestCol())

    ds = DataSet(project=project, batch_name="test")
    ds.set_dataset(data)
    ds.run()

    assert 1


####### Test anomaly detection ############
