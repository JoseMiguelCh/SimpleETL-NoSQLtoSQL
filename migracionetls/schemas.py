from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, TimestampType, ArrayType, LongType

# Codigos EPC
codigos_epc_hab_schema = StructType([
    StructField("id", StringType(), True),
    StructField("epc", StringType(), True),
    StructField("_ts", LongType(), True)
])

codigos_epc_hab_map = {
    'schema': codigos_epc_hab_schema,
    'destination_table_name': 'codigos_epc_hab',
    'has_auditoria': False
}

# Lista de usuarios
usuario_schema = ArrayType(StructType([
    StructField("id", StringType(), True),
    StructField("codigoIntermediador", IntegerType(), True),
    StructField("placa", StringType(), True),
    StructField("tid", StringType(), True),
    StructField("epc", StringType(), True),
    StructField("categoria", StringType(), True),
    StructField("estado", IntegerType(), True),
    StructField("estadoSaldo", IntegerType(), True),
    StructField("saldo", IntegerType(), True),
    StructField("saldoBajo", BooleanType(), True),
    StructField("numeroCliente", StringType(), True),
    StructField("modalidad", StringType(), True),
    StructField("version", TimestampType(), True), 
    StructField("auditoria", StructType([
        StructField("solicitante", IntegerType(), True),
        StructField("codigoCorrelacion", StringType(), True),
        StructField("fechaHoraServidor", TimestampType(), True),
        StructField("respondedor", IntegerType(), True),
        StructField("tiempoInicio", TimestampType(), True),
        StructField("tiempoFin", TimestampType(), True),
        StructField("idPrueba", IntegerType(), True)
    ]), True)
]), True)

usuarios_auditoria_schema = StructType([
    StructField("solicitante", IntegerType(), True),
    StructField("codigoCorrelacion", StringType(), True),
    StructField("fechaHoraServidor", TimestampType(), True),
    StructField("respondedor", IntegerType(), True),
    StructField("tiempoInicio", TimestampType(), True),
    StructField("tiempoFin", TimestampType(), True),
    StructField("idPrueba", IntegerType(), True)
])

lista_usuarios_hab = StructType([
    StructField("codigoIntermediador", IntegerType(), True),
    StructField("codigoLista", StringType(), True),
    StructField("tipoLista", StringType(), True),
    StructField("fechaEmision", TimestampType(), True),
    StructField("codigoListaAnterior", StringType(), True),
    StructField("numeroRegistros", IntegerType(), True),
    StructField("Usuarios", usuario_schema, True),
    StructField("auditoria", usuarios_auditoria_schema, True),
    StructField("id", StringType(), True),
    StructField("_rid", StringType(), True),
    StructField("_self", StringType(), True),
    StructField("_etag", StringType(), True),
    StructField("_attachments", StringType(), True),
    StructField("_ts", LongType(), True)
])

lista_usuarios_hab_map = {
    'schema': lista_usuarios_hab,
    'destination_table_name': 'lista_usuarios_hab',
    'has_auditoria': True,
    'details': [
        {
            'schema': usuario_schema,
            'column_name': 'Usuarios',
            'join_key': 'codigoLista',
            'destination_table_name': 'usuarios_hab', 
            'has_auditoria': True,
        }
    ]
}

# Negaciones
negaciones_paso_hab_schema = StructType([
    StructField("codigoNegacion", StringType(), True),
    StructField("codigoOperador", IntegerType(), True),
    StructField("fechaHora", StringType(), True),
    StructField("diaContable", StringType(), True),
    StructField("placa", StringType(), True),
    StructField("tid", StringType(), True),
    StructField("epc", StringType(), True),
    StructField("estacion", StringType(), True),
    StructField("carril", StringType(), True),
    StructField("tipoLectura", IntegerType(), True),
    StructField("sentido", StringType(), True),
    StructField("version", StringType(), True),
    StructField("codigoLista", StringType(), True),
    StructField("codigoRazon", IntegerType(), True),
    StructField("descripcionRazon", StringType(), True),
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
])

negaciones_paso_hab_map = {
    'schema': negaciones_paso_hab_schema,
    'destination_table_name': 'negaciones_pasos_hab',
    'has_auditoria': True
}

# Pruebas hab
ticket_schema = StructType([
    StructField("codigo", StringType(), True),
    StructField("fecha", StringType(), True),
    StructField("referencia", StringType(), True),
    StructField("codigoMotivo", IntegerType(), True),
    StructField("auditoria", StructType([
        StructField("solicitante", IntegerType(), True),
        StructField("codigoCorrelacion", StringType(), True),
        StructField("fechaHoraServidor", StringType(), True),
        StructField("respondedor", IntegerType(), True),
        StructField("tiempoInicio", StringType(), True),
        StructField("tiempoFin", StringType(), True),
        StructField("idPrueba", IntegerType(), True)
    ]), True)
])

pruebas_schema = ArrayType(StructType([
    StructField("href", StringType(), True),
    StructField("rel", StringType(), True)
]), True)

auditoria_schema = StructType([
    StructField("solicitante", IntegerType(), True),
    StructField("codigoCorrelacion", StringType(), True),
    StructField("fechaHoraServidor", StringType(), True),
    StructField("respondedor", IntegerType(), True),
    StructField("tiempoInicio", StringType(), True),
    StructField("tiempoFin", StringType(), True),
    StructField("idPrueba", IntegerType(), True)
])

pruebas_hab_schema = StructType([
    StructField("ticket", ticket_schema, True),
    StructField("codigoPaso", StringType(), True),
    StructField("codigoRespuesta", IntegerType(), True),
    StructField("pruebas", pruebas_schema, True),
    StructField("auditoria", auditoria_schema, True),
    StructField("id", StringType(), True),
    StructField("_rid", StringType(), True),
    StructField("_self", StringType(), True),
    StructField("_etag", StringType(), True),
    StructField("_attachments", StringType(), True),
    StructField("_ts", LongType(), True)
])

pruebas_hab_map = {
    'schema': pruebas_hab_schema,
    'destination_table_name': 'pruebas_hab',
    'has_auditoria': True,
    'details': [
        {
            'schema': ticket_schema,
            'column_name': 'ticket',
            'destination_table_name': 'ticket', 
            'has_auditoria': True,
        },
        {
            'schema': pruebas_schema,
            'column_name': 'pruebas',
            'destination_table_name': 'pruebas', 
            'has_auditoria': False,
        }
    ]
}

# Pasos hab
confirmaciones_procesamiento_schema = ArrayType(StructType([
    StructField("codigoPaso", StringType(), True),
    StructField("codigoIntermediador", IntegerType(), True),
    StructField("tokenConfirmacion", StringType(), True),
    StructField("codigoRespuesta", IntegerType(), True),
    StructField("descripcion", StringType(), True),
    StructField("auditoria", StructType([
        StructField("solicitante", IntegerType(), True),
        StructField("codigoCorrelacion", StringType(), True),
        StructField("fechaHoraServidor", TimestampType(), True),
        StructField("respondedor", IntegerType(), True),
        StructField("tiempoInicio", TimestampType(), True),
        StructField("tiempoFin", TimestampType(), True),
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

pasos_hab_schema = StructType([
    StructField("codigoPaso", StringType(), True),
    StructField("codigoOperador", IntegerType(), True),
    StructField("fechaHora", TimestampType(), True),
    StructField("fechaRecaudo", TimestampType(), True),
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
    StructField("fechaActualizacionUsuario", TimestampType(), True),
    StructField("codigoListaUsuario", StringType(), True),
    StructField("existeDiscrepanciaPlaca", BooleanType(), True),
    StructField("confirmacionesProcesamiento", confirmaciones_procesamiento_schema, True),
    StructField("auditoria", pasos_auditoria_schema, True),
    StructField("id", StringType(), True),
    StructField("_rid", StringType(), True),
    StructField("_self", StringType(), True),
    StructField("_etag", StringType(), True),
    StructField("_attachments", StringType(), True),
    StructField("_ts", LongType(), True)
])

pasos_hab_map = {
    'schema': pasos_hab_schema,
    'destination_table_name': 'pasos_hab',
    'has_auditoria': True,
    'details': [
        {
            'schema': confirmaciones_procesamiento_schema,
            'join_key': 'codigoPaso',
            'column_name': 'confirmacionesProcesamiento',
            'destination_table_name': 'confirmaciones_procesamiento_hab', 
            'has_auditoria': True,
        }
    ]
}

