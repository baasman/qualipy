FROM python:3.10

RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py \
    && python3 get-pip.py --force-reinstall \
    && apt-get update -y \
    && apt-get install -y python3-dev vim gcc 

ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8

COPY qualipy /src/qualipy
COPY requirements.txt setup.py MANIFEST.in /src/
RUN pip3 install /src