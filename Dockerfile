FROM ubuntu:18.04
LABEL maintainer="Fahd Benatia <fahd.benatia@csgroup.eu>"

WORKDIR work
COPY . /work

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

# Install system dependencies
RUN apt-get update -y && apt-get install -y python3-pip git gdal-bin \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
RUN pip3 install -U pip

## Install dataship and eotile
ADD eotile-0.2rc3-py3-none-any.whl /opt
RUN pip3 install /opt/eotile-0.2rc3-py3-none-any.whl


COPY dataship-0.1.4.tar.gz /opt
RUN pip3 install /opt/dataship-0.1.4.tar.gz

RUN pip3 install -U setuptools setuptools_scm wheel
RUN pip3 install .

RUN pip3 install boto3 \
  && pip3 install botocore \
  && pip3 install psycopg2-binary

ENTRYPOINT ["ewoc_l8"]