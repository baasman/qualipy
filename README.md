## Qualipy

Qualipy is a data monitoring and quality library. Most data quality tools out
there focus on monitoring the input and/or the output. Qualipy, on the other hand,
is completely focused on understanding your data during a pipeline or data workflow.

## Install

### from Git
```bash
git clone https://github.com/baasman/qualipy.git
pip install .
```
### from pip
```bash
pip install qualipy
```

## How to use

Qualipy allows you to set constraints on any particular column of a dataframe,
and run data quality aggregates on that column.

To monitor a column, all we need to do is create a mapping to that column.

```python
from qualipy import Column
from qualipy.backend.pandas_backend.pandas_types import FloatType

class SepalLength(Column):

    column_name = 'sepal_length'
    column_type = FloatType()
    force_type = True
    null = False
    force_null = False
    unique = False
```

## Running the webapp
```bash
cd web/

sudo docker build -t qualipy:latest .

sudo docker run -it -p 5006:5005 --mount type=bind,source=/home/<username>/.qualipy-prod/,target=/root/.qualipy qualipy:latest /bin/bash
```


