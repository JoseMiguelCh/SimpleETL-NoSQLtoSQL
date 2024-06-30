# Databricks notebook source
"""
Main module to run the ETL process. It extracts data from Cosmos DB, transforms
it and loads it into a Postgres database.
"""
# pylint: disable=import-error, c0206
import os
import logging
from dotenv import load_dotenv

from migracionetls.data_extraction.extract import extract_data
from migracionetls.data_transformation.transform import transform_data
from migracionetls.data_loading.load import load_data
from migracionetls.spark_utils import initialize_spark
from migracionetls.schemas_pasos import pasos_map

load_dotenv()
ENDPOINT = os.getenv("ENDPOINT")
MASTER_KEY = os.getenv("MASTER_KEY")

CONTAINERS_TO_EXTRACT = {
    'PasosNew': pasos_map
}
date_range = ("2024-05-20T00:00:00Z", "2024-05-20T00:05:00Z")

def main():
    """
    Main function to run the ETL process
    """
    logging.info("------ Initializing spark session -------")
    spark = initialize_spark(ENDPOINT, MASTER_KEY)
    for container in CONTAINERS_TO_EXTRACT:
        logging.info("------ Starting extraction for %s -------", container)
        logging.info("------ Extracting data from cosmos db -------")
        df = extract_data(spark, container, CONTAINERS_TO_EXTRACT[container]['schema'], date_range)
        logging.info("------ Transforming data -------")
        transformed_data = transform_data(df, CONTAINERS_TO_EXTRACT[container])
        logging.info("------ Loading data into PSQL -------")
        for source, target in transformed_data:
            load_data(source, target)

logging.basicConfig(level=logging.INFO)
main()
