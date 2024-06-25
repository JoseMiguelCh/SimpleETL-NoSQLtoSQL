"""
    Transform from nested to flat structure
"""
# pylint: disable=import-error
from pyspark.sql.functions import col, explode, explode_outer, from_unixtime, col, to_timestamp
from pyspark.sql.types import StructType, ArrayType


def rename_cols(df):
    """
    Rename the columns of a dataframe to have the first letter capitalized
    """
    def capitalize_first_letter(s):
        """ Capitalize the first letter of a string"""
        return s[0].upper() + s[1:]
    # Capitalize all column names
    df = df.select([col(c).alias(capitalize_first_letter(c))
                   for c in df.columns])
    return df


def transform_data(spark, df, container_map):
    """
    Transform the data from a nested structure to a flat structure
    """
    items = []
    # main_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), container_map.get('schema'))
    details = container_map.get('details')

    if details is not None:
        for detail in details:
            detail_column_name = detail['column_name']
            detail_destination_table_name = detail['destination_table_name']

            column_type = df.schema[detail_column_name].dataType
            if isinstance(column_type, StructType):
                detail_df = df.select("Id", f"{detail_column_name}.*")
            elif isinstance(column_type, ArrayType):
                join_key = detail.get('join_key', 'Id')
                detail_df = expand_array_into_struct(
                    df, join_key, detail_column_name)
            else:
                raise ValueError(f"Unsupported column type for {
                                 detail_column_name}")

            if detail.get('has_auditoria', False):
                detail_df, detail_auditoria = get_auditoria_df(detail_df)
                items.append((detail_df, detail_destination_table_name))
                items.append((detail_auditoria, "auditoria_" +
                             detail_destination_table_name))
            else:
                items.append((detail_df, detail_destination_table_name))
    destination_table_name = container_map['destination_table_name']
    has_auditoria = container_map['has_auditoria']
    columns_to_drop = [detail['column_name']
                       for detail in details] if details else []
    main_df = df.drop(*columns_to_drop)
    main_df.withColumn("TS2", from_unixtime(col("_ts")).cast("timestamp"))
    main_df.withColumn("FechaHora2",
                       to_timestamp(col("FechaHora"), "yyyy-MM-dd'T'HH:mm:ss.SSS"))

    if has_auditoria:
        principal_df, auditoria_df = get_auditoria_df(main_df)
        items.append((principal_df, destination_table_name))
        items.append((auditoria_df, "auditoria_" + destination_table_name))
    else:
        items.append((main_df, destination_table_name))

    return items


def get_auditoria_df(df):
    """
    Split the auditoria column into a separate dataframe
    """
    main_df = df.drop("auditoria")
    auditoria_df = df.select("Id", "auditoria.*")
    return main_df, auditoria_df


def expand_array_into_struct(df, join_key, array_column_name):
    """
    Expand an array column into a struct column
    """
    df = df.withColumn(array_column_name, explode(col(array_column_name)))
    df = df.select(join_key, f"{array_column_name}.*")
    return df
