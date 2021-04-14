FROM python:3.8

# Allow statements and log messages to immediately appear in the Cloud Run logs
ENV PYTHONUNBUFFERED True

COPY requirements.txt ./
COPY requirements-test.txt ./

RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir -r requirements-test.txt
RUN pip3 install --no-cache-dir  pylint

#copy dag code to container image
ENV DAGS /dags
WORKDIR $DAGS
ADD dag_validation.py /dag_validation.py
CMD ["pytest", "-s", "dag_validation.py"]


