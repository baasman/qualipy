from schema import Schema, And, Or, Optional


def list_or_tuple_of(sub_schema):
    return Or((sub_schema,), [sub_schema])


def type_and_list_of_each_type(*sub_schema):
    args = list(sub_schema) + [[schema] for schema in sub_schema]
    return Or(*args)


def var_or_none(*sub_schema):
    args = list(sub_schema) + [None]
    return Or(*args)


config_schema = Schema(
    {
        Optional("SCHEMA"): str,
        "QUALIPY_DB": str,
        Optional("COMPARISONS"): {
            str: {
                "projects": list,
                Optional("time_freq"): str,
                Optional("num_metrics"): list_or_tuple_of(list_or_tuple_of(str)),
                Optional("cat_metrics"): list_or_tuple_of(list_or_tuple_of(str)),
            }
        },
        Optional(str): {
            Optional("PROFILE_ARGS"): {
                Optional("facet_categorical_by"): str,
                Optional("facet_numerical_by"): str,
            },
            Optional("ANOMALY_ARGS"): {
                Optional("check_for_std"): bool,
                Optional("importance_level"): Or(float, int),
                Optional("distance_from_bound"): Or(float, int),
                Optional("specific"): {
                    str: {str: {"use": bool, Optional("severity"): Or(float, int)}}
                },
            },
            Optional("ANOMALY_MODEL"): str,
            Optional("DATE_FORMAT"): str,
            Optional("NUM_SEVERITY_LEVEL"): Or(float, int),
            Optional("CAT_SEVERITY_LEVEL"): Or(float, int),
            Optional("SEVERITY_LEVELS"): {str: Or(float, int)},
            Optional("VISUALIZATION"): {
                Optional("row_counts"): {
                    Optional("include_bar_of_latest"): {
                        "use": bool,
                        Optional("diff"): bool,
                        Optional("show_by_default"): bool,
                    },
                    Optional("include_summary"): {
                        "use": bool,
                        Optional("diff"): bool,
                        Optional("show_by_default"): bool,
                    },
                },
                Optional("trend"): {
                    Optional("include_bar_of_latest"): {
                        "use": bool,
                        Optional("diff"): bool,
                        Optional("show_by_default"): bool,
                        Optional("variables"): list_or_tuple_of(str),
                    },
                    Optional("include_summary"): {
                        "use": bool,
                        Optional("diff"): bool,
                        Optional("show_by_default"): bool,
                        Optional("variables"): list_or_tuple_of(str),
                    },
                    Optional("sst"): int,
                    Optional("point"): bool,
                    Optional("n_steps"): int,
                    Optional("add_diff"): {Optional("shift"): int},
                },
                Optional("missing"): {
                    Optional("include_0"): bool,
                    Optional("include_bar_of_latest"): {
                        "use": bool,
                        Optional("diff"): bool,
                        Optional("show_by_default"): bool,
                        Optional("variables"): list_or_tuple_of(str),
                    },
                },
                Optional("proportion"): {},
            },
            Optional("DISPLAY_NAMES"): {
                "DEFAULT": {Optional(str): {"display_name": str, "description": str}},
                Optional("CUSTOM"): {str: {"display_name": str, "description": str}},
            },
        },
    }
)
