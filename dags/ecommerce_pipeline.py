"""
E-commerce Data Pipeline DAG

1. Ingests raw CSV data into PostgreSQL staging tables
2. Runs dbt transformations and tests on the data warehouse
"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import subprocess
import os
import logging
from dotenv import load_dotenv

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Load environment variables from .env file (if available)
load_dotenv()

# Get DB credentials from environment
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "2409")
DB_HOST = os.getenv("DB_HOST", "postgres")  # Default to container name in Docker
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "ecommerce_analytics")
DB_SCHEMA = os.getenv("DB_SCHEMA", "dbt_dev")

# Get script & config paths
SCRIPTS_DIR = os.getenv("SCRIPTS_DIR", "/opt/airflow/scripts")
DBT_BINARY_PATH = os.getenv("DBT_BINARY_PATH", "/home/airflow/.local/bin/dbt")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", "/dbt-env")  # Match mounted volume
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/usr/app/dbt")  # Match mounted dbt project

# Validate required environment setup
required_vars = {
    "DB_USER": DB_USER,
    "DB_HOST": DB_HOST,
    "DB_NAME": DB_NAME,
}
missing_vars = [k for k, v in required_vars.items() if not v]
if missing_vars:
    raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")


def run_script(script_path: list, description: str):
    """
    Generic function to run a script or command with error handling.

    Args:
        script_path (list): List containing command and arguments
        description (str): Description of what the step does
    """
    logger.info(f"🚀 Starting: {description}")
    logger.info(f"Running command: {' '.join(script_path)}")
    try:
        result = subprocess.run(
            script_path,
            shell=False,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=os.environ.copy(),  # Pass full environment
            cwd=SCRIPTS_DIR  # Ensure consistent working directory
        )
        logger.info(f"✅ Success: {description}")
        if result.stdout:
            logger.debug(f"Output:\n{result.stdout}")
        return result
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Error in {description}:")
        logger.error(f"Command failed: {' '.join(e.cmd)}")
        logger.error(f"Exit code: {e.returncode}")
        logger.error(f"STDOUT:\n{e.stdout}")
        logger.error(f"STDERR:\n{e.stderr}")
        raise


def run_ingestion():
    """
    Task to run the ingestion script that loads raw datasets into the database.
    """
    ingestion_script = ["python", f"{SCRIPTS_DIR}/load_all_datasets.py"]
    logger.info(f"Running ingestion script at: {ingestion_script[1]}")
    run_script(ingestion_script, "Data Ingestion")


def run_dbt_transformations():
    """
    Task to trigger dbt transformations and run tests.
    Uses absolute path to avoid permission/path issues.
    """
    dbt_run = [
        DBT_BINARY_PATH,
        "run",
        "--profiles-dir", DBT_PROFILES_DIR,
        "--project-dir", DBT_PROJECT_DIR
    ]
    dbt_test = [
        DBT_BINARY_PATH,
        "test",
        "--profiles-dir", DBT_PROFILES_DIR,
        "--project-dir", DBT_PROJECT_DIR
    ]

    logger.info(f"Using dbt profiles dir: {DBT_PROFILES_DIR}")
    logger.info(f"Using dbt project dir: {DBT_PROJECT_DIR}")

    try:
        logger.info("Starting dbt run...")
        run_script(dbt_run, "dbt Transformations")
    except Exception as e:
        logger.error("Stopping pipeline due to failure in dbt run.")
        raise

    try:
        logger.info("Starting dbt test...")
        run_script(dbt_test, "dbt Tests")
    except Exception as e:
        logger.error("Stopping pipeline due to failure in dbt test.")
        raise


# Define DAG
dag = DAG(
    'ecommerce_pipeline',
    schedule_interval='@daily',  # Run daily
    start_date=datetime(2025, 1, 1),  # Future date for testing
    catchup=False,  # Disable backfilling
    description='Pipeline for Ecommerce Data Ingestion and Transformation',
)

# Define Tasks
t1 = PythonOperator(
    task_id='ingest_data',
    python_callable=run_ingestion,
    dag=dag,
    retries=3,
    retry_delay=30,
)

t2 = PythonOperator(
    task_id='transform_with_dbt',
    python_callable=run_dbt_transformations,
    dag=dag,
    retries=3,
    retry_delay=30,
)

# Task Dependencies
t1 >> t2