#!/usr/bin/env python3
import os
import logging
import sys
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Text, Integer, Numeric, DateTime

# Set up logging early
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Ensure current working directory is script dir
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# Try loading env vars early
dotenv_path = os.path.join(script_dir, '..', '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
    logger.info(f"📄 Loaded environment variables from: {dotenv_path}")
else:
    logger.warning("⚠️ No .env file found. Using default DB settings.")

# Log current working directory and data path
logger.info(f"📂 Current working directory: {os.getcwd()}")
data_dir = os.path.join(script_dir, '..', 'data')
logger.info(f"📁 Expected data directory: {data_dir}")

# Load environment variables with fallbacks
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "ecommerce_analytics")
DB_SCHEMA = os.getenv("DB_SCHEMA", "dbt_dev")

# Validate port is an integer
try:
    DB_PORT = int(DB_PORT)
except ValueError:
    logger.error(f"❌ DB_PORT must be a number. Got: {DB_PORT}")
    raise

# Build connection string
DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
logger.info(f"🔌 Constructed DB URL: {DB_URL[:30]}...")  # Mask password

# Create engine
try:
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        logger.info("✅ Successfully connected to the database.")
except Exception as e:
    logger.error(f"❌ Failed to connect to the database: {e}")
    raise

# Metadata with schema
metadata = MetaData(schema=DB_SCHEMA)


# Dataset mappings
DATASET_CONFIG = [
    {
        "filename": "olist_customers_dataset.csv",
        "table_name": "stg_customers",
        "date_cols": [],
        "columns": [Column("customer_id", Text), Column("customer_unique_id", Text),
                    Column("customer_zip_code_prefix", Text), Column("customer_city", Text),
                    Column("customer_state", Text)]
    },
    {
        "filename": "olist_geolocation_dataset.csv",
        "table_name": "stg_geolocation",
        "date_cols": [],
        "columns": [Column("geolocation_zip_code_prefix", Text), Column("geolocation_lat", Numeric),
                    Column("geolocation_lng", Numeric), Column("geolocation_city", Text),
                    Column("geolocation_state", Text)]
    },
    {
        "filename": "olist_order_items_dataset.csv",
        "table_name": "stg_order_items",
        "date_cols": ["shipping_limit_date"],
        "columns": [Column("order_id", Text), Column("order_item_id", Integer),
                    Column("product_id", Text), Column("seller_id", Text),
                    Column("shipping_limit_date", DateTime), Column("price", Numeric),
                    Column("freight_value", Numeric)]
    },
    {
        "filename": "olist_order_payments_dataset.csv",
        "table_name": "stg_order_payments",
        "date_cols": [],
        "columns": [Column("order_id", Text), Column("payment_sequential", Integer),
                    Column("payment_type", Text), Column("payment_installments", Integer),
                    Column("payment_value", Numeric)]
    },
    {
        "filename": "olist_order_reviews_dataset.csv",
        "table_name": "stg_order_reviews",
        "date_cols": ["review_creation_date", "review_answer_timestamp"],
        "columns": [Column("review_id", Text), Column("order_id", Text),
                    Column("review_score", Integer), Column("review_comment_title", Text),
                    Column("review_comment_message", Text), Column("review_creation_date", DateTime),
                    Column("review_answer_timestamp", DateTime)]
    },
    {
        "filename": "olist_orders_dataset.csv",
        "table_name": "stg_orders",
        "date_cols": ["order_purchase_timestamp", "order_approved_at",
                      "order_delivered_carrier_date", "order_delivered_customer_date",
                      "order_estimated_delivery_date"],
        "columns": [Column("order_id", Text), Column("customer_id", Text),
                    Column("order_status", Text), Column("order_purchase_timestamp", DateTime),
                    Column("order_approved_at", DateTime), Column("order_delivered_carrier_date", DateTime),
                    Column("order_delivered_customer_date", DateTime), Column("order_estimated_delivery_date", DateTime)]
    },
    {
        "filename": "olist_products_dataset.csv",
        "table_name": "stg_products",
        "date_cols": [],
        "columns": [Column("product_id", Text), Column("product_category_name", Text),
                    Column("product_name_length", Integer), Column("product_description_length", Integer),
                    Column("product_photos_qty", Integer), Column("product_weight_g", Integer),
                    Column("product_length_cm", Integer), Column("product_height_cm", Integer),
                    Column("product_width_cm", Integer)]
    },
    {
        "filename": "olist_sellers_dataset.csv",
        "table_name": "stg_sellers",
        "date_cols": [],
        "columns": [Column("seller_id", Text), Column("seller_zip_code_prefix", Text),
                    Column("seller_city", Text), Column("seller_state", Text)]
    },
    {
        "filename": "product_category_name_translation.csv",
        "table_name": "stg_product_category_translations",
        "date_cols": [],
        "columns": [Column("product_category_name", Text), Column("product_category_name_english", Text)]
    }
]

def create_table(table_name, columns):
    try:
        table = Table(table_name, metadata, *columns, extend_existing=True)
        table.create(bind=engine, checkfirst=True)
        logger.info(f"✅ Table {table_name} ensured to exist.")
    except Exception as e:
        logger.error(f"❌ Failed to create table {table_name}: {e}")
        raise

def load_dataset(config, data_dir):
    file_path = os.path.join(data_dir, config["filename"])
    table_name = config["table_name"]
    date_cols = config["date_cols"]

    if not os.path.isfile(file_path):
        logger.error(f"❌ File not found: {file_path}")
        return

    try:
        logger.info(f"🔄 Loading {file_path} into {table_name}...")
        df = pd.read_csv(file_path)
        df.columns = df.columns.str.replace(r'[^\w]', '_', regex=True).str.lower()
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        create_table(table_name, config["columns"])
        df.to_sql(table_name, engine, schema=DB_SCHEMA, if_exists='replace', index=False)
        logger.info(f"✅ Successfully loaded {file_path} into {table_name}")
    except Exception as e:
        logger.error(f"❌ Error loading {file_path}: {e}")

def main():
    data_dir = os.path.join(script_dir, '..', 'data')
    if not os.path.isdir(data_dir):
        logger.error(f"❌ Data directory '{data_dir}' does not exist.")
        logger.info("ℹ️ Please ensure the following files are present:")
        for config in DATASET_CONFIG:
            logger.info(f" - {config['filename']}")
        return

    logger.info(f"📁 Using data directory: {data_dir}")
    logger.info("🚀 Starting data ingestion process...")
    start_time = datetime.now()

    for config in DATASET_CONFIG:
        load_dataset(config, data_dir)

    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"🏁 Ingestion completed in {duration.total_seconds():.2f} seconds.")

if __name__ == "__main__":
    try:
        main()
    finally:
        engine.dispose()
        logger.info("🔌 Database engine disposed.")