.. highlight:: sh

===================
Configuration File
===================

The configuration file drives a lot of the reporting and anomaly detection in Qualipy. The default
config.json file is created upon running ``qualipy generate-config`` through the CLI.

Some notes on the configuration:
 * how to reference metrics...

The following json has all available keys one could set.

::

    {
        # The sqlalchemy like string that tells Qualipy where to store all data.
        # By default this is set to a sqlite file within the config directory
        "QUALIPY_DB": "sqlite:////tmp/.qualipy/qualipy.db"",
        # If using a database like postgres that supports schemas, setting this
        # would place all data in that specific schema
        "SCHEMA": "",
        # the name of the project were configuring. This corresponds to your Qualipy
        # pipeline.
        "example_project": {
            # This is where you specify anomaly specific settings
            "ANOMALY_ARGS": {
                # Each anomaly score corresponds to a standardized value. In general,
                # anything over 1 is considered an anomaly, but this could be used to 
                # control the severity of the outliers
                "importance_level": 1.3,
                # This is used to set "rules" for any specific column. See what rules
                # are available to use **here (set this)
                "specific": {
                    # to reference an aggregate, use run_name + column_name + metric_name + arguments (if any)
                    "rows_my_column_count_": {
                        # "increasing" is just an example of a function that checks whether
                        # or not the aggregate is always increasing. This might be useful
                        # when you're inspecting the total size of a database
                        "increasing": {
                            # Can be turned on and off
                            "use": true,
                            # Since this is not a machine learning based approach, you have
                            # to set your own severity level when using custom rules
                            "severity": 3
                        }
                    },
                }
            },
            # What anomaly model to use. See the Anomaly Detection guide for different
            # options
            "ANOMALY_MODEL": "prophet",
            # Date format to use on reports
            "DATE_FORMAT": "%Y-%m-%d",
            # Minimum severity level to set for filtering out numerical
            # anomalies on the anomaly report
            "NUM_SEVERITY_LEVEL": 1,
            # Minimum severity level to set for filtering out categorical
            # anomalies on the anomaly report
            "CAT_SEVERITY_LEVEL": 1,
            # Useful for categorizing anomalies based on certain thresholds
            "SEVERITY_LEVELS": {
                "low": 1.5,
                "medium": 2.5,
                "high": 10
            },
            # The following section controls the plots on the anomaly report
            "VISUALIZATION": {
                # Controls the visualizations that are displayed in the anomaly report. There
                # are 5 different categories of data to be displayed. Each one of them has their
                # own section

                # Since Qualipy by default gathers raw row counts for each data input, this section
                # will show show the overal trend of data size over time
                "row_counts": {

                    # Include this if you want to view the counts of the most recent batch.
                    "include_bar_of_latest": {
                        "use": true,
                        "diff": true,
                        "show_by_default": true
                    },
                    # Include this if you want to get a summary overview of the row counts
                    "include_summary": {
                        "use": true,
                        "show_by_default": true
                    }
                },

                # This section is for viewing all metrics that return a numerical data type,
                # such as float and int
                "trend": {
                    "include_bar_of_latest": {
                        "use": true,
                        # You can use this to only include certain metrics
                        "variables": [
                            "measurement_concept_id_measurement_number_of_unique_",
                            "drug_concept_id_drug_number_of_unique_",
                        ],
                        "diff": false,
                        "show_by_default": true
                    },
                    "include_summary": {
                        "use": true,
                        "show_by_default": true
                    },
                    # Specify an sst to add a layer to the plot that include_summary
                    # change point detection. The value refers to how far to look back
                    "sst": 3,
                    # Set this to true if each batch should have a point. Note, this
                    # can look unappealling with a large number of batches
                    "point": true,
                    # Set this to include a rolling mean for each trend
                    "n_steps": 10,
                    # Set this if you want to include a layer in the plot that shows
                    # the difference from a previous value
                    "add_diff": {
                        # Set this to determine how far to look back
                        "shift": 1
                    }
                },
                # Add this to visualize all categorical variables (those returning dicts
                # with counts).
                "proportion": {
                }
                # This section includes analysis on the missingness of the data
                "missing": {
                    # By default, it will only show data that contains any actual missing data.
                    # To also show data without any missingness, set this to True
                    "include_0": true,
                    "include_bar_of_latest": {
                        "use": true,
                        "diff": false
                    }
                },

            },

            # This section is for customizing the metric names and hover-over descriptions,
            # in order to potentially make them more human-readable
            "DISPLAY_NAMES": {
                # This default list is automatically populated by the function name
                # and description from the function definition
                "DEFAULT": {
                    "number_of_unique": {
                        "display_name": "number_of_unique_values",
                        "description": "A total count of the number of unique values in the batch"
                    }
                },
                "CUSTOM": {
                    "random_function": {
                        "display_name": "Random Function",
                        "description": "Description of random_function"
                    }
                },
            }
        },
    }



