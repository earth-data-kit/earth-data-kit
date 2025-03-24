FROM ghcr.io/osgeo/gdal:ubuntu-small-3.10.2

# Installing dependencies efficiently
RUN apt-get update && apt-get install -y \
    wget \
    python3-pip \
    python3-venv \
    net-tools \
    traceroute \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV VIRTUAL_ENV=/home/ubuntu/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Installing jupyter inside venv
RUN pip install jupyter

# Installing s5cmd
RUN wget https://github.com/peak/s5cmd/releases/download/v2.3.0/s5cmd_2.3.0_Linux-32bit.tar.gz -O /home/ubuntu/s5cmd_2.3.0_Linux-32bit.tar.gz
RUN tar -xzf /home/ubuntu/s5cmd_2.3.0_Linux-32bit.tar.gz -C /home/ubuntu/
RUN cp /home/ubuntu/s5cmd /usr/local/bin/
RUN chmod +x /usr/local/bin/s5cmd

# Installing earth_data_kit inside venv
COPY ./earth_data_kit-0.0.1a1.tar.gz /home/ubuntu/earth_data_kit-0.0.1a1.tar.gz
RUN pip install /home/ubuntu/earth_data_kit-0.0.1a1.tar.gz

# Fixing permissions
ARG NB_USER=ubuntu
ARG NB_UID=1000
ENV USER=${NB_USER}
ENV NB_UID=${NB_UID}
ENV HOME=/home/${NB_USER}

# Make sure the contents of our repo are in ${HOME}
COPY ./notebooks ${HOME}/notebooks
USER root
RUN chown -R ${NB_UID} ${HOME}
USER ${NB_USER}

# Setting working directory as /home/ubuntu/notebooks
WORKDIR ${HOME}/notebooks

CMD ["tail", "-f", "/dev/null"]