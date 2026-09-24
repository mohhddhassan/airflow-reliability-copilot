FROM apache/airflow:3.1.5-python3.12

USER root
# No extra OS packages required today; kept as a hook for future providers.
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY include/ /opt/airflow/include/
