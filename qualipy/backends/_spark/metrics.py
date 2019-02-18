import pyspark.sql.functions as F


def _get_mean(df, column):
    return df.agg(F.mean(F.col(column)).alias('mean')).first()['mean']


def _get_std(df, column):
    return df.agg(F.stddev(F.col(column)).alias('std')).first()['std']


def _get_nunique(df, column):
    return len(df.agg(F.collect_set(column).alias(column)).first()[column])

