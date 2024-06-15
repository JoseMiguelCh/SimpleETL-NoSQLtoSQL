"""
    Schema definitions for 'pasos' collections
"""
# pylint: disable=import-error
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, TimestampType, ArrayType, LongType

# Pasos hab
confirmaciones_procesamiento_schema = ArrayType(StructType([
    # StructField("codigoPaso", StringType(), True),
    StructField("codigoIntermediador", IntegerType(), True),
    StructField("tokenConfirmacion", StringType(), True),
    StructField("codigoRespuesta", IntegerType(), True),
    StructField("descripcion", StringType(), True),
    StructField("auditoria", StructType([
        StructField("solicitante", IntegerType(), True),
        StructField("codigoCorrelacion", StringType(), True),
        StructField("fechaHoraServidor", StringType(), True),
        StructField("respondedor", IntegerType(), True),
        StructField("tiempoInicio", StringType(), True),
        StructField("tiempoFin", StringType(), True),
        StructField("idPrueba", IntegerType(), True)
    ]), True),
    StructField("id", StringType(), True)
]), True)

pasos_auditoria_schema = StructType([
    StructField("solicitante", IntegerType(), True),
    StructField("codigoCorrelacion", StringType(), True),
    StructField("fechaHoraServidor", TimestampType(), True),
    StructField("respondedor", IntegerType(), True),
    StructField("tiempoInicio", TimestampType(), True),
    StructField("tiempoFin", TimestampType(), True),
    StructField("idPrueba", IntegerType(), True)
])

ajustes_pasos_schema = ArrayType(StructType([
    StructField("id", StringType(), True),
    StructField("codigoAjuste", StringType(), True),
    StructField("codigoOperador", IntegerType(), True),
    StructField("fechaHora", StringType(), True),
    StructField("fechaRecaudo", StringType(), True),
    StructField("codigoPasoReferencia", StringType(), True),
    StructField("motivoAjuste", IntegerType(), True),
    StructField("valorAjuste", LongType(), True),
    StructField("valor", LongType(), True),
    StructField("categoria", StringType(), True),
    StructField("tipoOperacion", IntegerType(), True),
    StructField("auditoria", StructType([
        StructField("solicitante", IntegerType(), True),
        StructField("codigoCorrelacion", StringType(), True),
        StructField("fechaHoraServidor", StringType(), True),
        StructField("respondedor", IntegerType(), True),
        StructField("tiempoInicio", StringType(), True),
        StructField("tiempoFin", StringType(), True),
        StructField("idPrueba", IntegerType(), True)
    ]), True)
]), True)

pasos_schema = StructType([
    StructField("id", StringType(), True),
    StructField("codigoPaso", StringType(), True),
    StructField("codigoOperador", IntegerType(), True),
    StructField("fechaHora", StringType(), True),
    StructField("fechaRecaudo", StringType(), True),
    StructField("codigoCliente", StringType(), True),
    StructField("placa", StringType(), True),
    StructField("tid", StringType(), True),
    StructField("epc", StringType(), True),
    StructField("estacion", StringType(), True),
    StructField("carril", StringType(), True),
    StructField("valor", IntegerType(), True),
    StructField("cantidadEjes", IntegerType(), True),
    StructField("cantidadDobleRuedas", IntegerType(), True),
    StructField("categoriaDAC", StringType(), True),
    StructField("categoriaCobrada", StringType(), True),
    StructField("tipoLectura", IntegerType(), True),
    StructField("sentido", StringType(), True),
    StructField("placaOCR", StringType(), True),
    StructField("existeDescrepancia", BooleanType(), True),
    StructField("fechaActualizacionUsuario", StringType(), True),
    StructField("codigoListaUsuario", StringType(), True),
    StructField("existeDiscrepanciaPlaca", BooleanType(), True),
    StructField("confirmacionesProcesamiento", confirmaciones_procesamiento_schema, True),
    StructField("ajustes", ajustes_pasos_schema, True),
    StructField("auditoria", pasos_auditoria_schema, True),
    StructField("_ts", IntegerType(), True),
])

pasos_map = {
    'schema': pasos_schema,
    'destination_table_name': 'Pasos',
    'has_auditoria': True,
    'details': [
        {
            'schema': confirmaciones_procesamiento_schema,
            'join_key': 'CodigoPaso',
            'column_name': 'ConfirmacionesProcesamiento',
            'destination_table_name': 'Confirmaciones_Pasos',
            'has_auditoria': True,
        },
        {
            'schema': ajustes_pasos_schema,
            'join_key': 'CodigoPaso',
            'column_name': 'Ajustes',
            'destination_table_name': 'Ajustes_Pasos',
            'has_auditoria': True,
        }
    ]
}
