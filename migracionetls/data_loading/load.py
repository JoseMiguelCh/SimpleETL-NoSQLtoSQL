import os
from dotenv import load_dotenv

load_dotenv()

def load_data(df, target_table):
    pg_properties = {
        "user": os.getenv("PG_USER"),
        "password": os.getenv("PG_PASSWORD"),
        "driver": "org.postgresql.Driver"
    }

    full_target_table = f"test_habilitacion.{target_table}"
    df.write \
        .jdbc(f"jdbc:postgresql://{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DATABASE')}",
        full_target_table, 
        mode="overwrite", 
        properties=pg_properties)