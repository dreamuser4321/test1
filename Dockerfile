############################################################################################################
# 
# 
# Base image for the whole Docker file
ARG LINUX_BASE_IMAGE="ubuntu:18.04"
############################################################################################################
# This is the development image to be used for DWH Miner project based on official Ubuntu 16.04 image
# Parameters:
#    LINUX_BASE_IMAGE - base python image (ubuntu:16.04)
############################################################################################################
FROM ${LINUX_BASE_IMAGE} as ubuntu-base-image
ENV LINUX_BASE_IMAGE=${LINUX_BASE_IMAGE}
ARG DEBIAN_FRONTEND=noninteractive

USER root

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

ARG MINER_VERSION="0.1.0"
ENV MINER_VERSION=${MINER_VERSION}

ARG MINER_USER=miner
ENV MINER_USER=${MINER_USER}

ARG MINER_USER_HOME="/home/${MINER_USER}"
ENV MINER_USER_HOME=${MINER_USER_HOME}

ARG DWH_MINER_DIR="${MINER_USER_HOME}/dwh_miner"
ENV DWH_MINER_DIR=${DWH_MINER_DIR}

ARG DWH_MINER_FILE_EXPORT_DIR="${DWH_MINER_DIR}/game_affinity"
ENV DWH_MINER_FILE_EXPORT_DIR=${DWH_MINER_FILE_EXPORT_DIR}

############################################################################################################
# OS packages to be installed
# - apt-utils
# - unzip
# - p7zip
# - unixodbc
# - unixodbc-dev
# - gcc
# - python3
# - python3-dev
# - curl
# - apt-transport-https
# - ca-certificates
# - msodbcsql17
# - python3-pip
# - pyodbc
# - boto3
# - pandas
# - pyarrow
# - sqlalchemy
#
############################################################################################################

# INSTALL DEPENDENCIES
RUN apt-get update \
    && apt-get install -y apt-utils \
    && apt-get install -y unzip \   
    && apt-get install -y p7zip \
    && apt-get install -y unixodbc \
    && apt-get install -y unixodbc-dev \
    && apt-get install -y gcc \
    && apt-get install -y python3 \
    && apt-get install -y python3-dev \
    && apt-get install -y gnupg \
    && apt-get install -y curl

# INSTALL MICROSOFT ODBC DRIVER 17
RUN apt-get update \
    && apt-get install -y curl \  
    && apt-get install -y apt-transport-https ca-certificates \
    && curl https://packages.microsoft.com/config/ubuntu/18.04/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get -y install msodbcsql17 \
    && apt-get install -y python3-pip \
    && pip3 install --upgrade pip pyodbc \
    && pip3 install --upgrade pip boto3 \
    && pip3 install --upgrade pip pandas \
    && pip3 install --upgrade pip pyarrow \
    && pip3 install --upgrade pip sqlalchemy \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

# Add a new user called ${MINER_USER}
RUN useradd -ms /bin/bash ${MINER_USER}

# Create a new directory ${DWH_MINER_DIR} and move all the code underneath
RUN mkdir ${DWH_MINER_DIR}
COPY . ${DWH_MINER_DIR}
# Change owner and file permissions
RUN chown -R ${MINER_USER} ${DWH_MINER_DIR}
RUN chmod u+x ${DWH_MINER_DIR}/execute_daemon_affinity.sh

# Switch to the ${MINER_USER} user and its working directory
USER ${MINER_USER}
WORKDIR ${MINER_USER_HOME}

# Create a new directory ${DWH_MINER_FILE_EXPORT_DIR} where to save the file exports
RUN mkdir ${DWH_MINER_FILE_EXPORT_DIR}

# Define entrypoint for the image
ENTRYPOINT ["./dwh_miner/execute_daemon_affinity.sh"]

RUN echo "Welcome to your MINER Docker container"
