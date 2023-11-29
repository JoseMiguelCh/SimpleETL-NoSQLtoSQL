from pyspark.sql import SparkSession

def initialize_spark(cosmos_enfpoint, cosmos_master_key):
    spark = SparkSession.builder \
        .appName("DataMigration") \
        .config("spark.jars", "jars/azure-cosmos-spark_3-4_2-12-4.23.0.jar, jars/postgresql-42.7.0.jar") \
        .getOrCreate()

    spark.conf.set("spark.sql.catalog.cosmosCatalog", "com.azure.cosmos.spark.CosmosCatalog")
    spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountEndpoint", cosmos_enfpoint)
    spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountKey", cosmos_master_key)
    
    return spark