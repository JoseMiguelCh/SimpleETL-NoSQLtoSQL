"""
    Transform from nested to flat structure
"""
# pylint: disable=import-error, unused-import
from pyspark.sql.functions import col, explode, from_unixtime, to_timestamp # type: ignore
from pyspark.sql.types import StructType, ArrayType # type: ignore


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


def transform_data(spark, df, container_map): # pylint: disable=unused-argument
    """
    Transform the data from a nested structure to a flat structure.
    """
    items = []
    details = container_map.get('details')
    df = rename_cols(df)

    if details is not None:
        for detail in details:
            detail_column_name = detail['column_name']
            detail_destination_table_name = detail['destination_table_name']

            column_type = df.schema[detail_column_name].dataType
            if isinstance(column_type, StructType):
                detail_df = df.select("Id", f"{detail_column_name}.*")
            elif isinstance(column_type, ArrayType):
                join_key = detail.get('join_key', 'Id')
                detail_df = expand_array_into_struct(df, join_key, detail_column_name)
            else:
                raise ValueError(f"Unsupported column type for {detail_column_name}")

            process_auditoria(detail_df, detail, items, detail_destination_table_name)

            if detail.get('details'):            
                print("Inner details", detail.get('details'))
                process_nested_details(detail_df, detail, items, ["Id"])

    destination_table_name = container_map['destination_table_name']
    has_auditoria = container_map['has_auditoria']
    columns_to_drop = [detail['column_name'] for detail in details] if details else []
    main_df = df.drop(*columns_to_drop)

    if has_auditoria:
        principal_df, auditoria_df = get_auditoria_df(main_df)
        items.append((principal_df, destination_table_name))
        items.append((auditoria_df, "auditoria_" + destination_table_name))
    else:
        items.append((main_df, destination_table_name))

    return items


def process_nested_details(df, detail, items, base_columns):
    """
    Process nested details recursively.
    """
    for inner_detail in detail.get('details', []):
        inner_detail_column_name = inner_detail['column_name']
        inner_detail_destination_table_name = inner_detail['destination_table_name']

        column_type = df.schema[inner_detail_column_name].dataType
        if isinstance(column_type, StructType):
            detail_df = df.select(*base_columns, f"{inner_detail_column_name}.*")
        elif isinstance(column_type, ArrayType):
            join_key = inner_detail.get('join_key', 'Id')
            detail_df = expand_array_into_struct(df, join_key, inner_detail_column_name)
        else:
            raise ValueError(f"Unsupported column type for {inner_detail_column_name}")
        detail_df = detail_df.drop(inner_detail_column_name)
        process_auditoria(detail_df, inner_detail, items, inner_detail_destination_table_name)
        if inner_detail.get('details'):
            new_base_columns = base_columns[:]
            if inner_detail_column_name not in new_base_columns:
                new_base_columns.append(inner_detail_column_name)
            process_nested_details(detail_df, inner_detail, items, new_base_columns)


def process_auditoria(df, detail, items, table_name):
    """
    Process auditoria information and append to items.
    """
    if detail.get('has_auditoria', False):
        detail_df, detail_auditoria = get_auditoria_df(df)
        items.append((detail_df, table_name))
        items.append((detail_auditoria, "auditoria_" + table_name))
    else:
        items.append((df, table_name))


def get_auditoria_df(df):
    """
    Split the auditoria column into a separate dataframe.
    """
    main_df = df.drop("auditoria")
    auditoria_df = df.select("Id", "auditoria.*")
    return main_df, auditoria_df


def expand_array_into_struct(df, join_key, array_column_name):
    """
    Expand an array column into a struct column.
    """
    df = df.withColumn(array_column_name, explode(col(array_column_name)))
    df = df.select(join_key, f"{array_column_name}.*")
    return df
