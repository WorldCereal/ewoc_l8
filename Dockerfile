FROM ubuntu:20.04
LABEL maintainer="Fahd Benatia <fahd.benatia@csgroup.eu>"

WORKDIR work
COPY . /work

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

# Install system dependencies
RUN apt-get update -y \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --fix-missing --no-install-recommends python3-pip git gdal-bin \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
RUN pip3 install --no-cache-dir -U pip

ARG EWOC_DAG_VERSION=0.4.0
LABEL EWOC_DAG_VERSION="${EWOC_DAG_VERSION}"
COPY ewoc-dag-${EWOC_DAG_VERSION}.tar.gz /opt
RUN pip3 install --no-cache-dir /opt/ewoc-dag-${EWOC_DAG_VERSION}.tar.gz

RUN pip3 install --no-cache-dir -U setuptools setuptools_scm wheel
RUN pip3 install --no-cache-dir .

RUN pip3 install --no-cache-dir boto3 \
  && pip3 install --no-cache-dir botocore \
  && pip3 install --no-cache-dir psycopg2-binary

ENTRYPOINT ["ewoc_l8"]
