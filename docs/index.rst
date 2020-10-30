.. qualipy documentation master file, created by
   sphinx-quickstart on Fri Aug 16 14:34:13 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Qualipy (v0.1.0)
===================================

Qualipy is a library designed to track and monitor real-time and retrospective data and provide automated
anomaly detection, reporting, and analysis on that data.

It does the following:
  * Provide a library that allows you to:
      * Create generic aggregate functions
      * Reflect any column that aggregates can be tracked on (Currently Pandas Series, Spark Columns and SQL columns)
      * Execute these aggregate functions as your data pipeline is running
  * Track and maintain these aggregate values in a separate location (Either SQlite or Postgres)
  * Generate reports describing your data in real time
      * Longitudinally describing all batches over time
      * Describe a single batch to understand at a deeper level
  * Run automated anomaly detection on all collected aggregates (and has an extendible anomaly detection model)
  * Provide a command line interface to:
      * Execute anomaly detection
      * Produce anomaly, comparison or batch reports
      * Interact with the historical data

Contents:

.. toctree::
   :maxdepth: 2

   installation.rst
   overview.rst
   tutorials.rst
   user_guide.rst
   configuration_file.rst
   CLI.rst
   anomaly_detection.rst



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

