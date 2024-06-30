"""
Additional transformations to apply to the data before loading it.
"""
import uuid
import logging
from pyspark.sql.functions import udf, col, to_timestamp, from_unixtime
from pyspark.sql.types import StringType

DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS"

@udf(StringType())
def generate_uuid(_):
    """Generate a UUID for the 'id' field."""
    return str(uuid.uuid4())

def additional_transformation(df, target_table_name):
    """
    Applies an additional transformation to the data before loading it.
    For this example, converts 'id' from text to UUID for target_table_name 'Pasos'.
    """
    logging.info("------ Applying additional transformation -------")
    logging.info("--- Transforming ----- {%s}", target_table_name)
    if target_table_name == 'Pasos':
        df = df.withColumn('Id', generate_uuid(df['id']))
        df = df.withColumn('FechaHora', to_timestamp(col('FechaHora'), DATETIME_FORMAT))
        df = df.withColumn('FechaRecaudo', to_timestamp(col('FechaRecaudo'), DATETIME_FORMAT))
        df = df.withColumn('FechaActualizacionUsuario', to_timestamp(col('FechaActualizacionUsuario'), DATETIME_FORMAT))
        df = df.withColumn('FechaCreacion', to_timestamp(from_unixtime(col('_ts')))).drop('_ts')
    if target_table_name == 'Confirmaciones_pasos':
        pass
    if target_table_name == 'Confirmaciones_ajustes':
        pass
    if target_table_name == 'Ajustes_pasos':
        pass
    if target_table_name == 'Auditoria_pasos':
        pass
    if target_table_name == 'Auditoria_confirmaciones_pasos':
        pass
    if target_table_name == 'Auditoria_confirmaciones_ajustes':
        pass
    if target_table_name == 'Auditoria_ajustes_pasos':
        pass
    return df