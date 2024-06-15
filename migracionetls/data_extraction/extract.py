"""
Extract data from Cosmos DB
"""
# pylint: disable=import-error
import os
from dotenv import load_dotenv

load_dotenv()
ENDPOINT = os.getenv("ENDPOINT")
MASTER_KEY = os.getenv("MASTER_KEY")
DATABASE_ID = os.getenv("DATABASE_ID")

def load_cosmos_config(container):
    """
    Load cosmos db configuration
    """
    cosmos_config = {
        "spark.cosmos.accountEndpoint": ENDPOINT,
        "spark.cosmos.accountKey": MASTER_KEY,
        "spark.cosmos.database": DATABASE_ID,
        "spark.cosmos.container": container
    }
    return cosmos_config

def extract_data(spark, container, schema, date_range):
    """
    Extract data from cosmos db
    """
    cosmos_config = load_cosmos_config(container)
    df = spark.read.format("cosmos.oltp") \
        .options(**cosmos_config) \
        .schema(schema) \
        .option("spark.cosmos.read.partitioning.strategy", "Restrictive") \
        .option("spark.cosmos.read.inferSchema.enabled", "true") \
        .option("spark.cosmos.read.customQuery", f"SELECT * FROM c WHERE c.fechaHora >= '{date_range[0]}' AND c.fechaHora <= '{date_range[1]}'") \
        .load()
    return df

    return df
