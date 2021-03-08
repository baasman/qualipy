DEFAULT_PROJECT_CONFIG = {
    "ANOMALY_ARGS": {
        "check_for_std": False,
        "importance_level": 1,
        "distance_from_bound": 1,
    },
    "PROFILE_ARGS": {},
    "ANOMALY_MODEL": "prophet",
    "DATE_FORMAT": "%Y-%m-%d",
    "NUM_SEVERITY_LEVEL": 1,
    "CAT_SEVERITY_LEVEL": 1,
    "VISUALIZATION": {
        "row_counts": {
            "include_bar_of_latest": {
                "use": True,
                "diff": False,
                "show_by_default": True,
            },
            "include_summary": {"use": True, "show_by_default": True},
        },
        "trend": {
            "include_bar_of_latest": {
                "use": True,
                "diff": False,
                "show_by_default": True,
            },
            "include_summary": {"use": True, "show_by_default": True},
            "sst": 3,
            "point": True,
            "n_steps": 10,
            "add_diff": {"shift": 1},
        },
        "missing": {
            "include_0": True,
            "include_bar_of_latest": {"use": True, "diff": False},
        },
    },
}
