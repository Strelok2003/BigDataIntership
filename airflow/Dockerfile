FROM apache/airflow:3.2.1

COPY requirements.txt /requirements.txt

USER root

RUN mkdir -p \
    /opt/airflow/Data/tmp \
    /opt/airflow/Data/cleaned \
    && chown -R airflow:root /opt/airflow/Data

USER airflow

RUN pip install --no-cache-dir -r /requirements.txt