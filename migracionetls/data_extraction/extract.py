import os
from dotenv import load_dotenv

load_dotenv()
ENDPOINT = os.getenv("ENDPOINT")
MASTER_KEY = os.getenv("MASTER_KEY")
DATABASE_ID = os.getenv("DATABASE_ID")

def load_cosmos_config(container):
    cosmosConfig = {
        "spark.cosmos.accountEndpoint": ENDPOINT,
        "spark.cosmos.accountKey": MASTER_KEY,
        "spark.cosmos.database": DATABASE_ID,
        "spark.cosmos.container": container
    }
    return cosmosConfig

def extract_data(spark, container, schema):
    cosmosConfig = load_cosmos_config(container)
    df = spark.read.format("cosmos.oltp") \
        .options(**cosmosConfig) \
        .schema(schema) \
        .load()

    return df
