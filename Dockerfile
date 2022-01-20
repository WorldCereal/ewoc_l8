# =========================================================================
#
#   Copyright 2021-22 (c) CS Group France. All rights reserved.
#
#   This file is part of S1Tiling project
#       https://gitlab.orfeo-toolbox.org/s1-tiling/s1tiling
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
# =========================================================================
#
# Authors: Fahd BENATIA (CS Group France)
#          Mickaël SAVINAUD (CS Group France)
#
# =========================================================================

FROM ubuntu:20.04
LABEL maintainer="CS GROUP France"
LABEL description="This docker allow to run ewoc_l8 processing chain."

WORKDIR work
COPY . /work

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

# Install system dependencies
RUN apt-get update -y \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --fix-missing --no-install-recommends \
    python3-pip \
    virtualenv \
    gdal-bin \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Update pip and create virtual env
RUN python3 -m pip install --no-cache-dir --upgrade pip \
    && python3 -m pip install --no-cache-dir virtualenv 

# Install python packages
ARG EWOC_DAG_VERSION=0.6.1
LABEL EWOC_DAG_VERSION="${EWOC_DAG_VERSION}"

# Copy private python packages
COPY ewoc_dag-${EWOC_DAG_VERSION}.tar.gz /tmp

SHELL ["/bin/bash", "-c"]

ENV EWOC_L8_VENV=/opt/ewoc_l8_venv
RUN python3 -m virtualenv ${EWOC_L8_VENV} \
    && source ${EWOC_L8_VENV}/bin/activate \
    && pip install --no-cache-dir /tmp/ewoc_dag-${EWOC_DAG_VERSION}.tar.gz \
    && pip install --no-cache-dir . \
    && pip install --no-cache-dir psycopg2-binary
# Last package useful for AGU script

ADD entrypoint.sh /opt
RUN chmod +x /opt/entrypoint.sh
ENTRYPOINT [ "/opt/entrypoint.sh" ]
