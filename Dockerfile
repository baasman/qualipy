FROM ubuntu:latest

MAINTAINER Boudewijn Aasman "boudeyz@gmail.com"

RUN apt-get update -y && \
    apt-get install -y python3-pip python3-dev vim

ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8


COPY qualipy /python/qualipy
WORKDIR /python/
COPY requirements.txt setup.py MANIFEST.in ./
COPY qualipy_web /python/qualipy_web
RUN pip3 install /python

WORKDIR /python

CMD ["qualipy run"]