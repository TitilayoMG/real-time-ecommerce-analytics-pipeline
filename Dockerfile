FROM apache/airflow:2.10.5

# install system packages as root
USER root
RUN apt-get update && apt-get install -y \
    gcc \
    librdkafka-dev \
 && rm -rf /var/lib/apt/lists/*

# switch to airflow user for pip installs
USER airflow

RUN pip install --default-timeout=300 --retries 3 --no-cache-dir \
    confluent-kafka==2.3.0 \
    "boto3>=1.34.0,<2.0.0" \
    psycopg2-binary==2.9.9 \
    dbt-core~=1.8.0 \
    dbt-redshift~=1.8.0