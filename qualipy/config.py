OVERVIEW_PAGE_COLUMNS = ["rows", "columns", "index"]
OVERVIEW_PAGE_METRICS_DEFAULT = ["perc_missing", "dtype", "is_unique"]
STANDARD_VIZ_STATIC = {
    "crosstab": {"function": "heatmap"},
    "correlation": {"function": "heatmap"},
}

STANDARD_VIZ_DYNAMIC = {"value_counts": {"function": "value_counts"}}
