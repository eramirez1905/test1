FROM python:3.8

# Allow statements and log messages to immediately appear in the Cloud Run logs
ENV PYTHONUNBUFFERED True
ENV CLI_VERSION=1.4.0
ENV DAGS /dags

COPY requirements.txt ./
COPY CICD_GITHUB.txt ./

# INSTALL REQUIREMENT AIRFLOW
RUN pip install --no-cache-dir -r requirements.txt

#INSTALL PYLINT
RUN pip3 install --no-cache-dir  pylint

# INSTALL GITHUB CLI
RUN apt update \
    && apt install -y git curl wget \
    && git version \
    && apt-get clean autoclean \
    && apt-get autoremove --yes \
    && rm -rf /var/lib/{apt,dpkg,cache,log}/
RUN wget -O- https://github.com/cli/cli/releases/download/v${CLI_VERSION}/gh_${CLI_VERSION}_linux_amd64.tar.gz | tar zx --strip-components=1

# ADD SCRIPT GITHUB TOKEN
ADD gh.bash /usr/bin
RUN chmod +x /usr/bin/gh.bash

# WORKDIR AIRFLOW
WORKDIR $DAGS

#ADD DAG VALIDATE TO AIRFLOW
ADD dag_validation.py /dags/dag_validation.py
CMD ["/usr/bin/gh.bash"]

