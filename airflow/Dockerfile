# dockerfile

FROM apache/airflow:3.1.3

USER root
RUN apt update -y
RUN apt install -y curl

USER airflow

COPY requirements.txt .
RUN pip install -r requirements.txt
