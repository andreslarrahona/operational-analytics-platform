# Use the official Apache Airflow image as the base
# Version 2.8.1 with Python 3.10 to match your docker-compose
FROM apache/airflow:2.8.1-python3.10

# Switch to root to install system-level dependencies if needed
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user for security (Best Practice)
USER airflow

# Install Python dependencies
# dbt-postgres: Core transformation engine
# pandas & sqlalchemy: For the E-L ingestion scripts
# psycopg2-binary: Postgres driver
RUN pip install --no-cache-dir \
    dbt-postgres==1.7.0 \
    pandas \
    sqlalchemy \
    psycopg2-binary

# Set dbt environment variables
# This ensures dbt knows where to look for the profiles.yml file
ENV DBT_PROFILES_DIR=/opt/airflow/dbt
ENV DBT_PROJECT_DIR=/opt/airflow/dbt

# (Optional) Copy your requirements file if you prefer managing many packages there
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt