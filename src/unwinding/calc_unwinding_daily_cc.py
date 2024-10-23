from pyspark.sql import functions as F
from db_connection_util import (
    get_spark_session,
    get_postgres_properties,
    read_data_from_postgres,
    update_to_database,
)
from src.functions.func_calc_unwinding import func_calc_unwinding


def calc_unwinding_daily_cc():
    try:
        spark = get_spark_session()
        postgres_properties = get_postgres_properties()
        schema_name = "ABACUS_A5"

        # Load necessary tables
        df_t14 = read_data_from_postgres(spark, "t14_unwinding", schema_name)
        df_w02 = read_data_from_postgres(spark, "w02_ccworking_day", schema_name)

        # Get the max working day for credit cards
        max_working_day = df_w02.agg(F.max("working_day")).collect()[0][0]

        # Apply the PySpark SQL-based unwinding calculation
        df_updated = func_calc_unwinding(df_t14, max_working_day)
        # Select relevant columns
        df_updated = df_updated.select(
            "autoid",
            "unwinding_daily_lcy",
        )

        # Write the final dataframe to a temporary table
        df_updated.write.jdbc(
            url=postgres_properties["url"],
            table='"ABACUS_A5"."temp_updated_rows"',
            mode="overwrite",  # Use overwrite for temporary table
            properties=postgres_properties,
        )

        update_query = """
                    UPDATE "ABACUS_A5"."t14_unwinding" AS t14
                    SET 
                       unwinding_daily_lcy = temp.unwinding_daily_lcy
                    FROM "ABACUS_A5"."temp_updated_rows" AS temp
                    WHERE t14.autoid = temp.autoid;
                    """

        update_to_database(update_query)

        print("calc_unwinding_daily_cc completed successfully.")
    except Exception as e:
        print(f"Error in calc_unwinding_daily_cc: {str(e)}")


if __name__ == "__main__":
    calc_unwinding_daily_cc()
