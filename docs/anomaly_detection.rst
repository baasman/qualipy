.. highlight:: sh

==================
Anomaly Detection
==================

Qualipy trains a separate anomaly and forecasting model for each aggregate that's generated
in your project. Each of these models are stored in the ``models`` directory of the configuration
directory, and can be deployed in any situation. It's generally up to the user on how to schedule
and deploy the training and reporting of anomalies, but Qualipy provides the functionalities to 
configure each model, fine-tune it's sensitivity to outliers, and store all anomalies for a project. It also
provides a variety of anomaly reports to understand and traverse through all anomalies in a project.

How to use
===========

In Qualipy, anomaly models can be controlled through the CLI and configuration file

First, we should cover the different available options in the configuration file to set up
our models. Note, it might be useful to first have a general understanding of the configuration layout **here**

The configuration for the anomaly models can be set within a the project specific config. To specify which
model we want to deploy, we must first set the ``ANOMALY_MODEL`` key::

    ...
    "my_project": {
        "ANOMALY_MODEL": "prophet"
    }
    ...

This means that Qualipy will train a separate ``prophet`` model for each metric. To further configure our prophet models,
we must dive into the ``ANOMALY_ARGS``. There are just a few options to set here.


+------------------+-----------------------------------------------------------------------------------------------------------+
| **Option**       | **Meaning**                                                                                               |
+==================+===========================================================================================================+
| importance_level | Sets the minimum required importance for the observations to be anomalous. As a rule of thumb, any value  |
|                  | greater than 1 is considered to be anomalous. Any value greater 3 is considered to be very anomalous. By  |
|                  | default, it is set to one.                                                                                |
+------------------+-----------------------------------------------------------------------------------------------------------+
| specific         | An area to set additional rules on a metric by metric basis. See more information here...                 |
+------------------+-----------------------------------------------------------------------------------------------------------+


Currently, there are three models implemented within Qualipy: prophet, std, and isolationForest. More info about each
of these can be found in the sections below.



Prophet
===================

isolationForest
===================

StandardDeviation
===================