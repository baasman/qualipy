FROM python:3.8-slim-buster

RUN apt-get update && apt-get install -y vim gcc python3-dev

RUN mkdir /workspace && \
    mkdir /src

ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8

RUN pip3 install jupyter && \
    jupyter notebook --generate-config && \
    echo "c.NotebookApp.ip = '0.0.0.0'" >> /root/.jupyter/jupyter_notebook_config.py

COPY qualipy /src/qualipy
COPY requirements.txt setup.py MANIFEST.in /src/
RUN pip3 install /src

WORKDIR /workspace