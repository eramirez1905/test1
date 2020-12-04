FROM apache/airflow:1.10.12-python3.7

USER root



ARG AIRFLOW_VERSION="1.10.12"
ARG AIRFLOW_EXTRAS="async,aws,gcp,kubernetes,mysql,postgres,redis,slack,ssh,statsd,virtualenv,emr,s3,gcp_api,google_auth"
#ARG ADDITIONAL_PYTHON_DEPS="requests-oauthlib==1.1.0 werkzeug<1.0.0 attrs~=19.3 marshmallow<4.0.0,>=3.0.0rc6 oauthlib==2.1.0  Flask-OAuthlib==0.9.5  protobuf>=3.12.0  grpcio==1.33.2  slackclient==2.0.0  google-cloud-dataproc==1.0.1  argcomplete==1.11.1  apache-airflow-backport-providers-google==2020.10.29  google-api-core==1.22.1  yarl  multidict"
ARG AIRFLOW_HOME=/opt/airflow
ARG PIP_VERSION="20.2.4"
ENV PIP_VERSION=${PIP_VERSION}

RUN pip install --upgrade pip

RUN set -ex \
    # https://airflow.readthedocs.io/en/latest/installation.html
    && su airflow -l -c "$(which pip) install --user apache-airflow[async,aws,gcp,kubernetes,mysql,postgres,redis,slack,ssh,statsd,virtualenv,emr,s3]==$AIRFLOW_VERSION \
      --constraint https://raw.githubusercontent.com/apache/airflow/constraints-master/constraints-3.7.txt" \
    && curl -L https://github.com/stedolan/jq/releases/download/jq-1.6/jq-linux64 -o /usr/local/bin/jq \
    && chmod +x /usr/local/bin/jq \
    && rm -rf \
        /opt/airflow/.cache \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

###https://raw.githubusercontent.com/apache/airflow/1.10.12/requirements/requirements-python3.7.txt
# 
# RUN pip install --user ${ADDITIONAL_PYTHON_DEPS} 

RUN curl -o /tmp/aws-iam-authenticator https://amazon-eks.s3.us-west-2.amazonaws.com/1.18.8/2020-09-18/bin/linux/amd64/aws-iam-authenticator \
    && chmod +x /tmp/aws-iam-authenticator \
    && mv /tmp/aws-iam-authenticator /usr/local/bin

USER airflow

COPY requirements.txt ${AIRFLOW_HOME}/requirements.txt
RUN pip install --user -r "${AIRFLOW_HOME}/requirements.txt" \
  && rm -rf /opt/airflow/.cache

COPY --chown=airflow docker/airflow ${AIRFLOW_HOME}/
COPY --chown=airflow dags ${AIRFLOW_HOME}/dags
COPY --chown=airflow src/datahub ${AIRFLOW_HOME}/src/datahub
COPY docker/rootdir /