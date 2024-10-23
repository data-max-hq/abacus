import os
from pyspark.sql import SparkSession
import psycopg2
import dotenv

dotenv.load_dotenv()

spark_jars = os.getenv("SPARK_JARS")
pg_url = os.getenv("PG_URL")
pg_user = os.getenv("PG_USER")
pg_pass = os.getenv("PG_PASS")
pg_host = os.getenv("PG_HOST")


def get_spark_session():
    return (
        SparkSession.builder.appName("PostgresToPySpark")
        .config("spark.jars", spark_jars)
        .config("spark.executor.memory", "2g")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.executor.memoryOverhead", "1g")
        .getOrCreate()
    )


def get_postgres_properties():
    return {
        "driver": "org.postgresql.Driver",
        "url": pg_url,
        "user": pg_user,
        "password": pg_pass,
    }


def read_data_from_postgres(spark, table_name, schema_name=None):
    properties = get_postgres_properties()
    full_table_name = (
        f'"{schema_name}"."{table_name}"' if schema_name else f'"{table_name}"'
    )
    df = spark.read.jdbc(
        url=properties["url"], table=full_table_name, properties=properties
    )
    return df


def update_to_database(query):
    # Use psycopg2 to perform the final update
    conn = psycopg2.connect(
        dbname="postgres",
        user=pg_user,
        password=pg_pass,
        host=pg_host,
        port="5432",
        options="-c search_path=ABACUS_A5",  # Set the schema for this connection
    )
    cur = conn.cursor()

    cur.execute(query)
    conn.commit()

    # Clean up
    cur.execute(""" DROP TABLE IF EXISTS "ABACUS_A5"."temp_updated_rows" """)
    conn.commit()

    cur.close()
    conn.close()
