from qualipy.reflect.function import function

import pyspark.sql.functions as F


@function(return_format=bool)
def is_unique(data, column):
    return (
        data.agg(F.countDistinct(column).alias("unique")).collect()[0][0]
        == data.count()
    )


@function(return_format=dict)
def value_counts(data, column):
    vc = data.groupBy(column).count().orderBy("count").toPandas()
    vc = {row[column]: row["count"] for idx, row in vc.iterrows()}
    return vc


@function(return_format=float)
def percentage_missing(data, column):
    missing = data.table_df.select(
        (
            F.count(F.when(F.isnan(column) | F.col(column).isNull(), column))
            / F.count(F.lit(1))
        ).alias(column)
    )
    total_count = data.table_df.count()
    try:
        perc = (total_count - missing.na.drop(subset=column).count()) / total_count
    except ZeroDivisionError:
        perc = None
    return perc


@function(return_format=bool)
def mean(data, column):
    return data.select(column).groupBy().mean().collect()[0]
