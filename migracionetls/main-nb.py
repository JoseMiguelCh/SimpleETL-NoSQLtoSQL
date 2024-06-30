# Databricks notebook source
"""
Main module to run the ETL process. It extracts data from Cosmos DB, transforms
it and loads it into a Postgres database.
"""
# pylint: disable=import-error, c0206
import os
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

from migracionetls.data_extraction.extract import extract_data
from migracionetls.data_transformation.transform import transform_data
from migracionetls.data_transformation.additional_transform import additional_transformation
from migracionetls.data_loading.load import load_data
from migracionetls.spark_utils import initialize_spark
from migracionetls.schemas_pasos import pasos_map

# Load environment variables
load_dotenv()
ENDPOINT = os.getenv("ENDPOINT")
MASTER_KEY = os.getenv("MASTER_KEY")
CONTAINERS_TO_EXTRACT = {
    'PasosNew': pasos_map
}

# Define the start and end dates for the total range and interval in days
start_date = "2024-05-20T00:00:00Z"
end_date = "2024-06-20T00:00:00Z"
interval_days = 1


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
        logging.error("Record counts do not match. Extracted: %d, Loaded: %d",
                      extracted_count, loaded_count)


def generate_date_ranges(start, end, days):
    """
    Generates a list of date ranges between start and end with the specified interval in days.
    """
    current_start = datetime.fromisoformat(start.replace("Z", "+00:00"))
    end_datetime = datetime.fromisoformat(end.replace("Z", "+00:00"))
    date_ranges = []
    while current_start < end_datetime:
        current_end = min(current_start + timedelta(days=days), end_datetime)
        date_ranges.append((current_start.isoformat() + "Z", current_end.isoformat() + "Z"))
        current_start = current_end
    return date_ranges


def main():
    """
    Main function to run the ETL process
    """
    logging.info("------ Initializing spark session -------")
    spark = initialize_spark(ENDPOINT, MASTER_KEY)

    logging.info("------ Generating date ranges -------")
    date_ranges = generate_date_ranges(start_date, end_date, interval_days)
    for container in CONTAINERS_TO_EXTRACT:
        for date_range in date_ranges:
            logging.info("------ Starting extraction for %s -------", container)
            logging.info("------ Extracting data from cosmos db for range: %s -------", date_range)
            
            df = extract_data(
                spark, container, CONTAINERS_TO_EXTRACT[container]['schema'], date_range)

            logging.info("------ Transforming data -------")
            transformed_data = transform_data(df, CONTAINERS_TO_EXTRACT[container])

            logging.info("------ Loading data into PostgreSQL -------")
            for data, target_table_name in transformed_data:
                add_trans_data = additional_transformation(data, target_table_name)
                load_data(add_trans_data, target_table_name)
                verify_counts(df, add_trans_data)
    logging.info("------ ETL process completed -------")


logging.basicConfig(level=logging.INFO)
main()
