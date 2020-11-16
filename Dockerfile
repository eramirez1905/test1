FROM apache/airflow:1.10.10-python3.7

USER root

RUN python -m pip install --upgrade pip

# RUN set -ex \
#     && packages=' \
#         build-essential \
#     ' \
#     && apt-get update -yqq \
#     && apt-get upgrade -yqq \
#     && apt-get install -yqq --no-install-recommends \
#       $packages \
#       jq procps \
#     # https://airflow.readthedocs.io/en/latest/installation.html
#     && su airflow -l -c "$(which pip) install --user apache-airflow[emr,s3,kubernetes,gcp_api,google_auth,statsd]==$AIRFLOW_VERSION \
#       --constraint https://raw.githubusercontent.com/apache/airflow/$AIRFLOW_VERSION/requirements/requirements-python3.7.txt" \
#     && apt-get purge --auto-remove -yqq $packages \
#     && apt-get autoremove -yqq --purge \
#     && apt-get clean \
#     && rm -rf \
#         /home/airflow/.cache \
#         /var/lib/apt/lists/* \
#         /tmp/* \
#         /var/tmp/* \
#         /usr/share/man \
#         /usr/share/doc \
#         /usr/share/doc-base
# Install basic and additional apt dependencies
RUN mkdir -pv /usr/share/man/man1 \
    && mkdir -pv /usr/share/man/man7 \
#     && export ${ADDITIONAL_DEV_ENV_VARS?} \
#     && bash -o pipefail -e -u -x -c "${DEV_APT_COMMAND}" \
#     && bash -o pipefail -e -u -x -c "${ADDITIONAL_DEV_APT_COMMAND}" \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
           apt-utils \
           build-essential \
           dirmngr \
           dumb-init \
           freetds-bin \
           freetds-dev \
           git \
           graphviz \
           gosu \
           libffi-dev \
           libkrb5-dev \
           libpq-dev \
           libsasl2-2 \
           libsasl2-dev \
           libsasl2-modules \
           libssl-dev \
           libenchant-dev \
           locales  \
           netcat \
           nodejs \
           rsync \
           sasl2-bin \
           sudo \
           unixodbc \
           unixodbc-dev \
           yarn \
#            ${ADDITIONAL_DEV_APT_DEPS} \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
    
COPY scripts/docker scripts/docker
RUN ./scripts/docker/install_mysql.sh dev

RUN adduser airflow \
    && echo "airflow ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/airflow \
    && chmod 0440 /etc/sudoers.d/airflow
    
# RUN curl -o /tmp/aws-iam-authenticator https://amazon-eks.s3.us-west-2.amazonaws.com/1.17.7/2020-07-08/bin/linux/amd64/aws-iam-authenticator \
#     && chmod +x /tmp/aws-iam-authenticator \
#     && mv /tmp/aws-iam-authenticator /usr/local/bin

# USER airflow

COPY requirements.txt ${AIRFLOW_HOME}/requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --user -r "${AIRFLOW_HOME}/requirements.txt" \
  && rm -rf /home/airflow/.cache

COPY --chown=airflow docker/airflow ${AIRFLOW_HOME}/
COPY --chown=airflow dags ${AIRFLOW_HOME}/dags
COPY --chown=airflow src/datahub ${AIRFLOW_HOME}/src/datahub
COPY docker/rootdir /
