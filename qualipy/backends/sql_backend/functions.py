from qualipy.reflect.function import function

import sqlalchemy as sa
import numpy as np


@function(return_format=float)
def mean(data, column):
    if data.custom_where is None:
        query = sa.select([sa.func.avg(sa.column(column))]).select_from(data._table)
    else:
        query = (
            sa.select([sa.func.avg(sa.column(column))])
            .select_from(data._table)
            .where(sa.text(data.custom_where))
        )
    res = data.engine.execute(query).scalar()
    if res is None:
        return np.NaN
    return float(data.engine.execute(query).scalar())


@function(return_format=float, allowed_arguments=["quantile"])
def median(data, column, quantile=0.5):
    select = sa.func.percentile_disc(quantile).within_group(sa.column(column).asc())
    if data.custom_where is None:
        query = sa.select([select]).select_from(data._table)
    else:
        query = (
            sa.select([select])
            .select_from(data._table)
            .where(sa.text(data.custom_where))
        )
    res = data.engine.execute(query).scalar()
    if res is None:
        return np.NaN
    try:
        return float(res)
    except:
        return np.NaN


def outside_of_range(data, column, low, high):
    if data.custom_where is None:
        res = data.engine.execute(
            sa.select([sa.func.count(sa.column(column))])
            .select_from(data._table)
            .where(sa.or_(sa.column(column) < low, sa.column(column) > high))
        ).scalar()
    else:
        res = data.engine.execute(
            sa.select([sa.func.count(sa.column(column))])
            .select_from(data._table)
            .where(
                sa.and_(
                    sa.or_(sa.column(column) < low, sa.column(column) > high),
                    sa.text(data.custom_where),
                )
            )
        ).scalar()
    if res is None:
        return np.NaN
    return res == 0


@function(return_format=float, allowed_arguments=["dedup_column"])
def dedup_mean(data, column, dedup_column):
    query = f"""
        with part_events as (
            select
                {column},
                row_number() over (partition by {dedup_column} order by {dedup_column}) as row_number
            from {data._table.fullname}
        )
        select avg({column})
        from part_events
        where row_number = 1
    """
    res = data.engine.execute(sa.text(query)).scalar()
    if res is None:
        return np.NaN
    return res


@function(return_format=dict, allowed_arguments=["dedup_column"])
def deduplicated_value_counts(data, column, dedup_column):
    # psql can use distinct on - much cleaner
    query = f"""
        with part_events as (
            select
                {column},
                row_number() over (partition by {dedup_column} order by {dedup_column}) as row_number
            from {data._table.fullname}
        )
        select {column}, count({column})
        from part_events
        where row_number = 1
        group by {column}
    """
    counts = data.engine.execute(sa.text(query)).fetchall()
    counts = {i[0]: i[1] for i in counts}
    return counts


@function(return_format=float)
def prop_outside_of_range(data, column, low, high):
    if data.custom_where is None:
        count_query = sa.select([sa.func.count()]).select_from(data._table)
        total_rows = int(data.engine.execute(count_query).scalar())
        res = data.engine.execute(
            sa.select([sa.func.count(sa.column(column))])
            .select_from(data._table)
            .where(sa.or_(sa.column(column) < low, sa.column(column) > high))
        ).scalar()
    else:
        count_query = (
            sa.select([sa.func.count()])
            .select_from(data._table)
            .where(sa.text(data.custom_where))
        )
        total_rows = int(data.engine.execute(count_query).scalar())
        res = data.engine.execute(
            sa.select([sa.func.count(sa.column(column))])
            .select_from(data._table)
            .where(
                sa.and_(
                    sa.or_(sa.column(column) < low, sa.column(column) > high),
                    sa.text(data.custom_where),
                )
            )
        ).scalar()

    if res is None or total_rows == 0:
        return np.NaN

    return res / total_rows


@function(return_format=dict, allowed_arguments=["always_run", "keep_top"])
def value_counts(data, column, always_run=True, keep_top=25):
    schema_name = "" if data.schema is None else data.schema + "."
    if data.dialect == "oracle":
        check_unique_cases = f"""
            select count(distinct {column}) from 
                (select {column} from {schema_name}{data.table_name} fetch first 1000 rows only)
        """

    else:
        check_unique_cases = f"""
            select count(distinct {column}) from 
                (select {column} from {schema_name}{data.table_name} limit 1000) as fin
        """
    count_distinct = data.engine.execute(check_unique_cases).fetchone()[0]
    if count_distinct < 25 or always_run:
        if data.custom_where is None:
            counts = data.engine.execute(
                sa.select([sa.column(column), sa.func.count(sa.column(column))])
                .select_from(data._table)
                .group_by(sa.column(column))
            ).fetchall()
        else:
            counts = data.engine.execute(
                sa.select([sa.column(column), sa.func.count(sa.column(column))])
                .select_from(data._table)
                .where(sa.text(data.custom_where))
                .group_by(sa.column(column))
            ).fetchall()
        if keep_top is not None:
            counts.sort(reverse=True, key=lambda x: x[1])
            counts = counts[:keep_top]
        counts = {i[0]: i[1] for i in counts}
    else:
        counts = np.NaN
    return counts


@function(return_format=int)
def distinct_count(data, column):
    if data.custom_where is None:
        counts = data.engine.execute(
            sa.select(
                [
                    sa.func.count(sa.distinct(sa.column(column))),
                ]
            ).select_from(data._table)
        ).fetchall()
    else:
        counts = data.engine.execute(
            sa.select(
                [
                    sa.func.count(sa.distinct(sa.column(column))),
                ]
            )
            .select_from(data._table)
            .where(sa.text(data.custom_where))
        ).fetchall()
    return counts[0][0]


@function(
    return_format=float,
    display_name="Percentage Missing",
    description="This function finds the percentage of values set to Null in the column",
)
def percentage_missing(data, column):
    try:
        if data.custom_where is None:
            counts = data.engine.execute(
                sa.select(
                    [sa.func.count(sa.text("*")), sa.func.count(sa.column(column))]
                ).select_from(data._table)
            ).fetchall()
        else:
            counts = data.engine.execute(
                sa.select(
                    [sa.func.count(sa.text("*")), sa.func.count(sa.column(column))]
                )
                .select_from(data._table)
                .where(sa.text(data.custom_where))
            ).fetchall()
    except:
        return np.NaN
    total = counts[0][0]
    missing = counts[0][1]
    if total == 0:
        return np.NaN
    return (total - missing) / total


@function(return_format=bool)
def is_unique(data, column):
    if data.custom_where is None:
        counts = data.engine.execute(
            sa.select(
                [
                    sa.func.count(sa.distinct(sa.column(column))),
                    sa.func.count(sa.column(column)),
                ]
            ).select_from(data._table)
        ).fetchall()
    else:
        counts = data.engine.execute(
            sa.select(
                [
                    sa.func.count(sa.distinct(sa.column(column))),
                    sa.func.count(sa.column(column)),
                ]
            )
            .select_from(data._table)
            .where(sa.text(data.custom_where))
        ).fetchall()

    distinct = counts[0][0]
    total = counts[0][1]
    return distinct == total


@function(return_format=int)
def number_of_unique(data, column):
    counts = data.engine.execute(
        sa.select(
            [
                sa.func.count(sa.distinct(sa.column(column))),
            ]
        ).select_from(data._table)
    ).fetchall()

    distinct = counts[0][0]
    return distinct
