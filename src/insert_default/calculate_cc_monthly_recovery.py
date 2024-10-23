from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, lit, sum, upper, date_format, coalesce
from db_connection_util import (
    get_spark_session,
    get_postgres_properties,
    read_data_from_postgres,
    update_to_database,
)


def calculate_cc_monthly_recovery():
    try:
        spark = get_spark_session()
        postgres_properties = get_postgres_properties()
        schema_name = "ABACUS_A5"

        # Load necessary tables
        df_t17 = read_data_from_postgres(
            spark, "atmp_t17_dpd_credit_cards", schema_name
        )
        df_t26 = read_data_from_postgres(spark, "t26_writteoff_recovery", schema_name)
        df_w01 = read_data_from_postgres(spark, "w01_working_day", schema_name)

        # Get the max working day
        max_working_day = df_w01.agg(F.max("working_day")).collect()[0][0]

        # Calculate daily_payment
        df_t26_sum = (
            df_t26.filter(
                (col("working_day") == lit(max_working_day))
                & (upper(col("writteoff_recovery")).isin(["WA", "WO", "REP", "DS"]))
            )
            .groupBy("id_product")
            .agg(sum("amount_event").alias("sum_amount_event"))
            .select(col("id_product").alias("sum_id_product"), col("sum_amount_event"))
        )

        df_t17 = df_t17.join(
            df_t26_sum, df_t17.id_product == df_t26_sum.sum_id_product, "left"
        ).withColumn(
            "daily_payment",
            when(
                col("p_amount_past_due") > col("amount_past_due"),
                col("p_amount_past_due")
                - col("amount_past_due")
                - coalesce(col("sum_amount_event"), lit(0)),
            ).otherwise(0),
        )

        # Calculate monthly_recover, monthly_recover_default, and monthly_recover_absorbing
        df_t17 = (
            df_t17.withColumn(
                "monthly_recover",
                when(
                    date_format(col("p_working_day"), "dd") == "01",
                    col("daily_payment"),
                ).otherwise(
                    coalesce(col("p_monthly_recover"), lit(0)) + col("daily_payment")
                ),
            )
            .withColumn(
                "monthly_recover_default",
                when(
                    (date_format(col("p_working_day"), "dd") == "01")
                    & (col("default_indicator") == "1"),
                    col("daily_payment"),
                )
                .when(
                    (date_format(col("p_working_day"), "dd") == "01")
                    & (col("default_indicator") == "0"),
                    0,
                )
                .when(
                    col("default_indicator") == "1",
                    coalesce(col("p_monthly_recover_default"), lit(0))
                    + col("daily_payment"),
                )
                .otherwise(coalesce(col("p_monthly_recover_default"), lit(0))),
            )
            .withColumn(
                "monthly_recover_absorbing",
                when(
                    (date_format(col("p_working_day"), "dd") == "01")
                    & (col("absorbing_status_flag") == "1"),
                    col("daily_payment"),
                )
                .when(
                    (date_format(col("p_working_day"), "dd") == "01")
                    | (col("absorbing_status_flag") == "0"),
                    0,
                )
                .when(
                    col("absorbing_status_flag") == "1",
                    coalesce(col("p_monthly_recover_absorbing"), lit(0))
                    + col("daily_payment"),
                )
                .otherwise(0),
            )
        )

        # Remove joined columns and aliases
        df_t17 = df_t17.drop("sum_id_product", "sum_amount_event")
        df_t17 = df_t17.select(*[col(c).alias(c) for c in df_t17.columns])

        df_updated = df_t17.select(
            "id_product",
            "daily_payment",
            "monthly_recover",
            "monthly_recover_default",
            "monthly_recover_absorbing",
        )

        # Write the final dataframe to a temporary table
        df_updated.write.jdbc(
            url=postgres_properties["url"],
            table='"ABACUS_A5"."temp_updated_rows"',
            mode="overwrite",  # Use overwrite for temporary table
            properties=postgres_properties,
        )

        update_query = """
                UPDATE "ABACUS_A5"."atmp_t17_dpd_credit_cards" AS t17
                SET 
                    daily_payment = temp.daily_payment,
                    monthly_recover = temp.monthly_recover,
                    monthly_recover_default = temp.monthly_recover_default,
                    monthly_recover_absorbing = temp.monthly_recover_absorbing
                FROM "ABACUS_A5"."temp_updated_rows" AS temp
                WHERE t17.id_product = temp.id_product;
                """

        update_to_database(update_query)

        print("calculate_cc_monthly_recovery completed successfully.")
    except Exception as e:
        print(f"Error in calculate_cc_monthly_recovery: {str(e)}")


if __name__ == "__main__":
    calculate_cc_monthly_recovery()
