from qualipy.reflect.function import function

import pyspark.sql.functions as F
import numpy as np


@function(
    return_format="custom",
    custom_value_return_format=float,
    allowed_arguments=["category"],
)
def aggregate_per_category(data, column, category):
    table = data.table_df
    table = table.repartition(20, category)
    aggs = table.groupby(category).agg(
        F.min(column).alias("min"),
        F.max(column).alias("max"),
        F.avg(column).alias("average"),
    )
    aggs = aggs.toPandas()
    custom_meas = []
    for idx, row in aggs.iterrows():
        for metric in ["min", "max", "average"]:
            if row[metric] is None:
                value = np.NaN
            else:
                value = float(row[metric])
            custom_meas.append(
                {
                    "value": value,
                    "run_name": str(row[category]),
                    "metric_name": metric,
                }
            )
    return custom_meas