from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit
from db_connection_util import (
    get_spark_session,
    get_postgres_properties,
    read_data_from_postgres,
    update_to_database,
)
from src.functions.func_calc_unwinding_mtd import (
    func_calc_unwinding_mtd,
)  # Import the function from the separate file


def calc_unwinding_mtd_cc():
    try:
        # Get Spark session and Postgres properties
        spark = get_spark_session()
        postgres_properties = get_postgres_properties()
        schema_name = "ABACUS_A5"

        # Load necessary tables
        df_t14 = read_data_from_postgres(spark, "t14_unwinding", schema_name)
        df_w02 = read_data_from_postgres(spark, "w02_ccworking_day", schema_name)

        # Get the max working day for credit cards
        max_working_day = df_w02.agg(F.max("working_day")).collect()[0][0]

        # Filter relevant rows
        df_filtered = df_t14.filter(
            (col("product_group") == "CC")
            & (col("working_day") == lit(max_working_day))
        )

        # Apply the PySpark function for calculating unwinding MTD
        df_updated = func_calc_unwinding_mtd(df_filtered)

        # Select relevant columns
        df_updated = df_updated.select("autoid", "unwinding_mtd_lcy")

        # Write the updated dataframe to a temporary table
        df_updated.write.jdbc(
            url=postgres_properties["url"],
            table='"ABACUS_A5"."temp_updated_rows"',
            mode="overwrite",
            properties=postgres_properties,
        )

        # Update the final table with the new values
        update_query = """
                        UPDATE "ABACUS_A5"."t14_unwinding" AS t14
                        SET 
                           unwinding_mtd_lcy = temp.unwinding_mtd_lcy
                        FROM "ABACUS_A5"."temp_updated_rows" AS temp
                        WHERE t14.autoid = temp.autoid;
                       """

        update_to_database(update_query)

        print("calc_unwinding_mtd_cc completed successfully.")

    except Exception as e:
        print(f"Error in calc_unwinding_mtd_cc: {str(e)}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    calc_unwinding_mtd_cc()
