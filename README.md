# E-commerce Analytics Pipeline

A production-ready data pipeline that ingests raw e-commerce datasets into PostgreSQL, transforms them using **dbt (Data Build Tool)**, and enables powerful analytics. Built with **Apache Airflow** for orchestration and Docker for reproducibility.

![Airflow](https://img.shields.io/badge/Airflow-2.9.3-orange)
![dbt](https://img.shields.io/badge/Tool-dbt-blue)
![PostgreSQL](https://img.shields.io/badge/Database-PostgreSQL-336791)
![Docker](https://img.shields.io/badge/Container-Docker-2496ED)

---

## Features
- **Automated DAG** in Airflow to orchestrate ingestion and transformation
- Ingests **9+ raw CSV datasets** (customers, orders, products, sellers, reviews, etc.)
- Loads data into **PostgreSQL staging tables** with proper schema and data types
- Applies **dbt transformations** to build clean marts and business logic
- Includes **data quality tests** via dbt
- Fully containerized with **Docker & docker-compose**
- Pre-configured `init.sql` for database initialization
- Utility script to preview transformed tables
- Supports environment variables for secure configuration

---

## Requirements
- **Docker** and **Docker Compose**
- **Git**
- At least 4GB RAM (recommended for smooth container performance)

---

## Setup & Installation

### 1. Clone the repo
```bash
git clone https://github.com/Treespunking/Ecommerce-Analytics-Pipeline.git
cd Ecommerce-Analytics-Pipeline
```

### 2. Create `.env` file
Copy the example environment file:
```bash
cp .env.example .env
```
> Modify values if needed (e.g., passwords, ports)

Example `.env`:
```env
DB_USER=postgres
DB_PASSWORD=2409
DB_NAME=ecommerce_analytics
DB_SCHEMA=dbt_dev
```

### 3. Build and start containers
```bash
docker-compose up --build
```
> This will:
> - Build the custom Airflow image with dbt
> - Start PostgreSQL, Redis, and Airflow services
> - Initialize the database
> - Create an Airflow admin user

Wait until all services are healthy (especially `airflow-init`).

---

## Access the Tools

### 1. Airflow Web UI
Open in browser:
```
http://localhost:8080
```
Login with:
- **Username:** `admin`
- **Password:** `admin`

The `ecommerce_pipeline` DAG will run daily (or trigger manually).

---

### 2. Explore Transformed Data

After the DAG runs successfully, inspect the final tables:

```bash
python first10rows_from_each_processed_table.py
```

> This script connects to PostgreSQL and prints the first 10 rows of each table in the `dbt_dev` schema.

---

## dbt Transformations

The pipeline uses **dbt Core** to transform raw data into analytics-ready models:

### Key Models
- `stg_*`: Staging models (direct from CSVs)
- `dim_customers`, `dim_products`, `dim_sellers`: Dimension tables
- `fct_orders`, `fct_order_items`: Fact tables
- Business-level marts (e.g., customer lifetime value, product performance)

### Data Tests
- Unique key constraints
- Not-null checks
- Referential integrity (foreign keys)
- Custom business rules

Run dbt manually (inside container):
```bash
docker exec -it dbt bash
cd /usr/app/dbt
dbt run --profiles-dir /root/.dbt
dbt test --profiles-dir /root/.dbt
```

---

## Project Structure
```
Ecommerce-Analytics-Pipeline/
│
├── docker-compose.yml         # Multi-container orchestration
├── Dockerfile                 # Custom Airflow + dbt image
├── .env                       # Environment variables
├── .env.example               # Template for .env
│
├── dags/
│   └── ecommerce_pipeline.py  # Airflow DAG for orchestration
│
├── data/                      # Raw CSV datasets (mounted to containers)
│   ├── olist_customers_dataset.csv
│   ├── olist_orders_dataset.csv
│   └── ... (9+ files)
│
├── scripts/
│   ├── load_all_datasets.py   # Ingestion script
│   ├── init.sql               # DB initialization
│   └── first10rows_from_each_processed_table.py  # Data preview tool
│
├── dbt-profiles/              # dbt profiles.yml (mounted)
│
├── ecommerce_dbt/             # dbt project
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/
│   │   ├── marts/
│   │   └── ...
│   └── ...
│
└── README.md
```

---

## How It Works

1. **Airflow Scheduler** triggers the `ecommerce_pipeline` DAG
2. **Task 1: Ingest Data**
   - Runs `load_all_datasets.py`
   - Creates staging tables in PostgreSQL
   - Loads and cleans CSV data
3. **Task 2: Transform with dbt**
   - Executes `dbt run` to build models
   - Applies transformations and business logic
   - Runs `dbt test` for data quality validation
4. **Output**: Clean, documented, and tested data warehouse ready for BI tools.

---

## Sample Use Cases
- Customer segmentation and retention analysis
- Product performance and category trends
- Order fulfillment and delivery time analysis
- Seller performance benchmarking
- Revenue forecasting and cohort analysis

Connect tools like **Metabase**, **Tableau**, or **Power BI** to `postgresql://localhost:5432/ecommerce_analytics` for visualization.

---
