FROM ghcr.io/osgeo/gdal:ubuntu-small-3.10.3

WORKDIR /app

# Setup python and poetry
RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-venv

RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install poetry

# Getting s5cmd
RUN apt-get install -y wget
RUN wget -O s5cmd.tar.gz https://github.com/peak/s5cmd/releases/download/v2.3.0/s5cmd_2.3.0_Linux-64bit.tar.gz
RUN tar -xzf s5cmd.tar.gz -C /bin s5cmd

# Building EDK
WORKDIR /app/earth-data-kit

COPY setup.py /app/earth-data-kit/setup.py
COPY pyproject.toml /app/earth-data-kit/pyproject.toml
COPY poetry.lock /app/earth-data-kit/poetry.lock
COPY README.md /app/earth-data-kit/README.md
COPY LICENSE.md /app/earth-data-kit/LICENSE.md

COPY earth_data_kit /app/earth-data-kit/earth_data_kit

RUN poetry build

# Installing EDK
RUN PKG_FILE=$(ls dist/*.tar.gz | head -n 1) && pip install "$PKG_FILE"

# Copy requirements.txt from the workspace directory if it exists and install it
ARG WORKSPACE_DIR
ENV WORKSPACE_DIR=${WORKSPACE_DIR}
COPY ${WORKSPACE_DIR}/requirements.txt /app/workspace-requirements.txt

RUN pip install -r /app/workspace-requirements.txt

WORKDIR /app/workspace