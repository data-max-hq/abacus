from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, lit
from db_connection_util import (
    get_spark_session,
    get_postgres_properties,
    read_data_from_postgres,
    update_to_database,
)


def calc_unwinding_ocy_cc():
    try:
        spark = get_spark_session()
        postgres_properties = get_postgres_properties()
        schema_name = "ABACUS_A5"

        # Load necessary tables
        df_t14 = read_data_from_postgres(spark, "t14_unwinding", schema_name)
        df_t20 = read_data_from_postgres(spark, "t20_exchange_rate", schema_name)
        df_w02 = read_data_from_postgres(spark, "w02_ccworking_day", schema_name)

        # Get the max working day for credit cards
        max_working_day = df_w02.agg(F.max("working_day")).collect()[0][0]

        # Select only needed columns from t20_exchange_rate with aliases
        df_t20_filtered = df_t20.select(
            col("currency").alias("t20_currency"),
            col("working_day").alias("t20_working_day"),
            col("all_rate").alias("t20_all_rate"),
        )

        # Perform the join operation
        df_updated = (
            df_t14.join(
                df_t20_filtered,
                (df_t14.deal_ccy == col("t20_currency"))
                & (df_t14.working_day == col("t20_working_day")),
                "left",
            )
            .withColumn(
                "unwinding_daily_ocy",
                when(
                    (col("product_group") == "CC")
                    & (col("working_day") == lit(max_working_day)),
                    col("unwinding_daily_lcy") / col("t20_all_rate"),
                ).otherwise(col("unwinding_daily_ocy")),
            )
            .withColumn(
                "unwinding_mtd_ocy",
                when(
                    (col("product_group") == "CC")
                    & (col("working_day") == lit(max_working_day)),
                    col("unwinding_mtd_lcy") / col("t20_all_rate"),
                ).otherwise(col("unwinding_mtd_ocy")),
            )
        )

        # Remove the joined columns and aliases
        df_updated = df_updated.drop("t20_currency", "t20_working_day", "t20_all_rate")
        df_updated = df_updated.select(*[col(c).alias(c) for c in df_t14.columns])

        df_updated = df_updated.select(
            "autoid", "unwinding_daily_ocy", "unwinding_mtd_ocy"
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
                               unwinding_daily_ocy = temp.unwinding_daily_ocy,
                               unwinding_mtd_ocy = temp.unwinding_mtd_ocy
                            FROM "ABACUS_A5"."temp_updated_rows" AS temp
                            WHERE t14.autoid = temp.autoid;
                            """

        update_to_database(update_query)

        print("calc_unwinding_ocy_cc completed successfully.")
    except Exception as e:
        print(f"Error in calc_unwinding_ocy_cc: {str(e)}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    calc_unwinding_ocy_cc()
