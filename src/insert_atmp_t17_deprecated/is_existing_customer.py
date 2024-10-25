from pyspark.sql.functions import col
from db_connection_util import (
    get_spark_session,
    get_postgres_properties,
    read_data_from_postgres,
)


def is_existing_customer(inrp_customer):
    try:
        spark = get_spark_session()
        get_postgres_properties()
        schema_name = "ABACUS_A5"

        # Read t01_customer table
        df_t01 = read_data_from_postgres(spark, "t01_customer", schema_name)

        # Count matching rows
        noexist = df_t01.filter(col("nrp_customer") == inrp_customer).count()

        return noexist
    except Exception as e:
        print(f"Error in isexistingcustomer: {str(e)}")
        import traceback

        traceback.print_exc()
        return None
