"""
Main module to run the ETL process. It extracts data from Cosmos DB, transforms it and loads it into a Postgres database.
"""
# pylint: disable=import-error
import os
import logging
from dotenv import load_dotenv

from migracionetls.data_extraction.extract import extract_data
from migracionetls.data_transformation.transform import transform_data
from migracionetls.data_loading.load import load_data
from migracionetls.spark_utils import initialize_spark
from migracionetls.schemas import (codigos_epc_hab_map, lista_usuarios_hab_map, 
    negaciones_paso_hab_map, pasos_hab_map, pruebas_hab_map)

load_dotenv()
ENDPOINT = os.getenv("ENDPOINT")
MASTER_KEY = os.getenv("MASTER_KEY")

CONTAINERS_TO_EXTRACT = {
    #'CodigosEPCHab': codigos_epc_hab_map,
    #'ListaUsuariosHab': lista_usuarios_hab_map,
    #'NegacionesPasoHab': negaciones_paso_hab_map,
    #'PruebasHab': pruebas_hab_map,
    'PasosHab': pasos_hab_map
}
date_range = ("2024-05-20T00:00:00Z", "2024-05-20T00:05:00Z")

def main():
    """
    Main function to run the ETL process
    """
    logging.info("------ Initializing spark session -------")
    spark = initialize_spark(ENDPOINT, MASTER_KEY)
    # pylint: disable=c0206
    for container in CONTAINERS_TO_EXTRACT:
        logging.info("------ Starting extraction for %s -------", container)
        logging.info("------ Extracting data from cosmos db -------")
        df = extract_data(spark, container, CONTAINERS_TO_EXTRACT[container]['schema'], None)
        logging.info("------ Transforming data -------")
        transformed_data = transform_data(spark, df, CONTAINERS_TO_EXTRACT[container])
        logging.info("------ Loading data into PSQL -------")
        [load_data(source, target) for source, target in transformed_data]
    
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
