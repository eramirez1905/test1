FROM python:3.8

# Allow statements and log messages to immediately appear in the Cloud Run logs
ENV PYTHONUNBUFFERED True
ENV DAGS /dags

# WORKDIR AIRFLOW
WORKDIR $DAGS

COPY requirements.txt ./

# INSTALL REQUIREMENT AIRFLOW
RUN pip install --no-cache-dir -r requirements.txt

#INSTALL PYLINT
RUN pip3 install --no-cache-dir  pylint


#ADD DAG VALIDATE TO AIRFLOW
ADD dag_validation.py /dags/dag_validation.py