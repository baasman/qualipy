FROM ubuntu:latest

MAINTAINER Boudewijn Aasman "boudeyz@gmail.com"

RUN apt-get update -y && \
    apt-get install -y python3-pip python3-dev vim

ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8

COPY qualipy /python/qualipy
COPY requirements.txt /python/requirements.txt
COPY setup.py /python/setup.py
RUN pip3 install .

COPY web /python/web
RUN pip3 install -r /python/web/requirements.txt

WORKDIR /python/web

CMD ["flask run"]