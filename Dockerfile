FROM apache/airflow:2.1.4

COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt --no-cache-dir