.. highlight:: sh

Installation and Setup
=======================


To install using pip::

    $ pip install qualipy

To install using git::

    $ git clone https://github.com/baasman/qualipy
    $ cd qualipy
    $ pip install .

By default, Qualipy will create a SQLite file to store and maintain all data. However,
as data grows larger and more complex, Postgres becomes recommended. This is not a guide
on running Postgres, but to get setup easily, docker is recommended.