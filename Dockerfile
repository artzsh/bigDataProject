FROM apache/airflow:2.10.3
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" g4f
