# Use official Airflow image as base
FROM apache/airflow:2.9.3

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow

# Switch to root to install dependencies
USER root

# Install system dependencies if needed (e.g., gcc for psycopg2)
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc python3-dev && \
    rm -rf /var/lib/apt/lists/*

# Revert to airflow user
USER airflow

# Upgrade pip and install Python dependencies
RUN pip install --upgrade pip && \
    pip install pandas sqlalchemy python-dotenv psycopg2-binary

# 🚀 Add DBT installation
RUN pip install dbt-core dbt-postgres

# Create necessary directories
RUN mkdir -p $AIRFLOW_HOME/dags $AIRFLOW_HOME/logs $AIRFLOW_HOME/plugins $AIRFLOW_HOME/data $AIRFLOW_HOME/scripts

# Set working directory
WORKDIR $AIRFLOW_HOME

# Copy DAG files
COPY dags/ $AIRFLOW_HOME/dags/

# Copy data folder (contains all 9 CSVs)
COPY data/ $AIRFLOW_HOME/data/

# Copy scripts folder (contains load_all_datasets.py)
COPY scripts/ $AIRFLOW_HOME/scripts/

# Optional: Copy .env file
COPY .env $AIRFLOW_HOME/.env

# Expose PostgreSQL port (for local testing)
EXPOSE 5432

# Default command is set by Airflow base image