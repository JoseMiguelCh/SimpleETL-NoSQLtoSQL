# Databricks notebook source
"""
Main module to run the ETL process. It extracts data from Cosmos DB, transforms
it and loads it into a Postgres database.
"""
# pylint: disable=import-error, c0206
import os
import uuid
import logging
from dotenv import load_dotenv
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

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

def verify_counts(extracted_df, loaded_df):
    """
    Verify if the count of records in the extracted DataFrame matches the 
    count of records in the loaded DataFrame.
    """
    extracted_count = extracted_df.count()
    loaded_count = loaded_df.count()
    if extracted_count == loaded_count:
        logging.info("Record counts match: %d records.", extracted_count)
    else:
        logging.error("Record counts do not match. Extracted: %d, Loaded: %d", extracted_count, loaded_count)

def uuid_udf():
    """ Generate a UUID for the 'id' field."""
    return str(uuid.uuid4())

def additional_transformation(df, target):
    """
    Applies an additional transformation to the data before loading it.
    For this example, converts 'id' from text to UUID for target 'Pasos'.
    """
    if target == 'Pasos':
        uuid_generate = udf(uuid_udf, StringType())
        df = df.withColumn('id', uuid_generate())
        df.show()
    return df

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

        # Apply an additional transformation step
        logging.info("------ Applying additional transformation -------")
        additional_transformed_data = additional_transformation(transformed_data, container)

        logging.info("------ Loading data into PSQL -------")
        for data, target in additional_transformed_data:
            load_data(data, target)
            verify_counts(df, data)
        logging.info("------ ETL process completed -------")
logging.basicConfig(level=logging.INFO)
main()
