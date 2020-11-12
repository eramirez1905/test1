# FROM apache/airflow:1.10.10-python3.7

# USER root

# RUN python -m pip install --upgrade pip

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

# # RUN curl -o /tmp/aws-iam-authenticator https://amazon-eks.s3.us-west-2.amazonaws.com/1.17.7/2020-07-08/bin/linux/amd64/aws-iam-authenticator \
# #     && chmod +x /tmp/aws-iam-authenticator \
# #     && mv /tmp/aws-iam-authenticator /usr/local/bin

# USER airflow

# COPY requirements.txt ${AIRFLOW_HOME}/requirements.txt
# RUN pip install --user -r "${AIRFLOW_HOME}/requirements.txt" \
#   && rm -rf /home/airflow/.cache

# COPY --chown=airflow docker/airflow ${AIRFLOW_HOME}/
# COPY --chown=airflow dags ${AIRFLOW_HOME}/dags
# COPY --chown=airflow src/datahub ${AIRFLOW_HOME}/src/datahub
# COPY docker/rootdir /

##############################################################################################
# This is the actual Airflow image - much smaller than the build one. We copy
# installed Airflow and all it's dependencies from the build image to make it smaller.
##############################################################################################
ARG PYTHON_BASE_IMAGE="python:3.6-slim-buster"

FROM ${PYTHON_BASE_IMAGE} as main
SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

ARG AIRFLOW_UID
ARG AIRFLOW_GID

LABEL org.apache.airflow.distro="debian"
LABEL org.apache.airflow.distro.version="buster"
LABEL org.apache.airflow.module="airflow"
LABEL org.apache.airflow.component="airflow"
LABEL org.apache.airflow.image="airflow"
LABEL org.apache.airflow.uid="${AIRFLOW_UID}"
LABEL org.apache.airflow.gid="${AIRFLOW_GID}"

ARG PYTHON_BASE_IMAGE
ENV PYTHON_BASE_IMAGE=${PYTHON_BASE_IMAGE}

ARG AIRFLOW_VERSION
ENV AIRFLOW_VERSION=${AIRFLOW_VERSION}

ARG ADDITIONAL_RUNTIME_DEPS=""
ENV ADDITIONAL_RUNTIME_DEPS=${ADDITIONAL_RUNTIME_DEPS}

# Make sure noninteractive debian install is used and language variables set
ENV DEBIAN_FRONTEND=noninteractive LANGUAGE=C.UTF-8 LANG=C.UTF-8 LC_ALL=C.UTF-8 \
    LC_CTYPE=C.UTF-8 LC_MESSAGES=C.UTF-8

# Note missing man directories on debian-buster
# https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=863199
# Install basic and additional apt dependencies
RUN mkdir -pv /usr/share/man/man1 \
    && mkdir -pv /usr/share/man/man7 \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
           apt-transport-https \
           apt-utils \
           ca-certificates \
           curl \
           dumb-init \
           freetds-bin \
           gnupg \
           gosu \
           krb5-user \
           ldap-utils \
           libffi6 \
           libsasl2-2 \
           libsasl2-modules \
           libssl1.1 \
           locales  \
           lsb-release \
           netcat \
           openssh-client \
           postgresql-client \
           rsync \
           sasl2-bin \
           sqlite3 \
           sudo \
           unixodbc \
           ${ADDITIONAL_RUNTIME_DEPS} \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install MySQL client from Oracle repositories (Debian installs mariadb)
RUN KEY="A4A9406876FCBD3C456770C88C718D3B5072E1F5" \
    && GNUPGHOME="$(mktemp -d)" \
    && export GNUPGHOME \
    && for KEYSERVER in $(shuf -e \
            ha.pool.sks-keyservers.net \
            hkp://p80.pool.sks-keyservers.net:80 \
            keyserver.ubuntu.com \
            hkp://keyserver.ubuntu.com:80 \
            pgp.mit.edu) ; do \
          gpg --keyserver "${KEYSERVER}" --recv-keys "${KEY}" && break || true ; \
       done \
    && gpg --export "${KEY}" | apt-key add - \
    && gpgconf --kill all \
    rm -rf "${GNUPGHOME}"; \
    apt-key list > /dev/null \
    && echo "deb http://repo.mysql.com/apt/debian/ stretch mysql-5.7" | tee -a /etc/apt/sources.list.d/mysql.list \
    && apt-get update \
    && apt-get install --no-install-recommends -y \
        libmysqlclient20 \
        mysql-client \
    && apt-get autoremove -yqq --purge \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV AIRFLOW_UID=${AIRFLOW_UID}
ENV AIRFLOW_GID=${AIRFLOW_GID}

ENV AIRFLOW__CORE__LOAD_EXAMPLES="false"

ARG AIRFLOW_USER_HOME_DIR=/home/airflow
ENV AIRFLOW_USER_HOME_DIR=${AIRFLOW_USER_HOME_DIR}

RUN addgroup --gid "${AIRFLOW_GID}" "airflow" && \
    adduser --quiet "airflow" --uid "${AIRFLOW_UID}" \
        --gid "${AIRFLOW_GID}" \
        --home "${AIRFLOW_USER_HOME_DIR}"

ARG AIRFLOW_HOME
ENV AIRFLOW_HOME=${AIRFLOW_HOME}

# Make Airflow files belong to the root group and are accessible. This is to accomodate the guidelines from
# OpenShift https://docs.openshift.com/enterprise/3.0/creating_images/guidelines.html
RUN mkdir -pv "${AIRFLOW_HOME}"; \
    mkdir -pv "${AIRFLOW_HOME}/dags"; \
    mkdir -pv "${AIRFLOW_HOME}/logs"; \
    chown -R "airflow:root" "${AIRFLOW_USER_HOME_DIR}" "${AIRFLOW_HOME}"; \
    find "${AIRFLOW_HOME}" -executable -print0 | xargs --null chmod g+x && \
        find "${AIRFLOW_HOME}" -print0 | xargs --null chmod g+rw


COPY --chown=airflow:root --from=airflow-build-image /root/.local "${AIRFLOW_USER_HOME_DIR}/.local"

COPY --chown=airflow:root scripts/in_container/prod/entrypoint_prod.sh /entrypoint
COPY --chown=airflow:root scripts/in_container/prod/clean-logs.sh /clean-logs
RUN chmod a+x /entrypoint /clean-logs

COPY --chown=airflow docker/airflow ${AIRFLOW_HOME}/
COPY --chown=airflow dags ${AIRFLOW_HOME}/dags
COPY --chown=airflow src/datahub ${AIRFLOW_HOME}/src/datahub
COPY docker/rootdir /

# Make /etc/passwd root-group-writeable so that user can be dynamically added by OpenShift
# See https://github.com/apache/airflow/issues/9248
RUN chmod g=u /etc/passwd

ENV PATH="${AIRFLOW_USER_HOME_DIR}/.local/bin:${PATH}"
ENV GUNICORN_CMD_ARGS="--worker-tmp-dir /dev/shm"

WORKDIR ${AIRFLOW_HOME}

EXPOSE 8080

USER ${AIRFLOW_UID}

ARG BUILD_ID
ENV BUILD_ID=${BUILD_ID}
ARG COMMIT_SHA
ENV COMMIT_SHA=${COMMIT_SHA}

LABEL org.apache.airflow.distro="debian"
LABEL org.apache.airflow.distro.version="buster"
LABEL org.apache.airflow.module="airflow"
LABEL org.apache.airflow.component="airflow"
LABEL org.apache.airflow.image="airflow"
LABEL org.apache.airflow.uid="${AIRFLOW_UID}"
LABEL org.apache.airflow.gid="${AIRFLOW_GID}"
LABEL org.apache.airflow.mainImage.buildId=${BUILD_ID}
LABEL org.apache.airflow.mainImage.commitSha=${COMMIT_SHA}

ENTRYPOINT ["/usr/bin/dumb-init", "--", "/entrypoint"]
CMD ["--help"]
