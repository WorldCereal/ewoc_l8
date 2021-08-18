FROM ubuntu:18.04
LABEL maintainer="Fahd Benatia <fahd.benatia@csgroup.eu>"

WORKDIR work
COPY . /work

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

## Install dataship and eotile
RUN apt-get update -y && apt-get install -y python3-pip && apt-get install -y git
RUN pip3 install -U pip
ADD eotile-0.2rc3-py3-none-any.whl /opt
RUN pip3 install /opt/eotile-0.2rc3-py3-none-any.whl


COPY dataship-0.1.4.tar.gz /opt
RUN pip3 install /opt/dataship-0.1.4.tar.gz
#Install gdal bins
RUN apt-get install -y gdal-bin

RUN pip3 install -U setuptools setuptools_scm wheel
RUN pip3 install .

RUN pip3 install boto3 \
  && pip3 install botocore \
  && pip3 install psycopg2-binary

ENTRYPOINT ["ewoc_l8"]