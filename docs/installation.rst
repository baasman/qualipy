.. highlight:: sh

Installation and Setup
=======================

I highly recommend using conda to simplify the installation of fbprophet/pystan, but 
any package manager will work. fbprophet is not a required library, but it provides the best
anomaly detection results.

To create a new virtual environment::

    $ conda create --name qpy python=3
    $ conda activate qpy

To install using pip::

    $ pip install qualipy
    $ conda install -c conda-forge pystan
    $ conda install -c conda-forge fbprophet

To install using git::

    $ git clone https://github.com/baasman/qualipy
    $ cd qualipy
    $ pip install .
    $ conda install -c conda-forge pystan
    $ conda install -c conda-forge fbprophet

By default, Qualipy will create a SQLite file to store and maintain all data. However,
as data grows larger and more complex, Postgres becomes recommended. This is not a guide
on running Postgres, but to get setup easily, docker is recommended.