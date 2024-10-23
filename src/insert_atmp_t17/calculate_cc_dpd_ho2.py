from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, round, lit, datediff

from db_connection_util import (
    get_spark_session,
    get_postgres_properties,
    read_data_from_postgres,
    update_to_database,
)
from src.functions.func_date_since_pd_ho2_cc import func_date_since_pd_ho2_cc


def calculate_cc_dpd_ho2():
    try:
        spark = get_spark_session()
        postgres_properties = get_postgres_properties()
        schema_name = "ABACUS_A5"

        # Load necessary tables
        df_t17 = read_data_from_postgres(
            spark, "atmp_t17_dpd_credit_cards", schema_name
        )
        df_t22 = read_data_from_postgres(spark, "t22_card_balance", schema_name)
        df_t20 = read_data_from_postgres(spark, "t20_exchange_rate", schema_name)
        df_t01 = read_data_from_postgres(spark, "atmp_customer", schema_name)
        df_w01 = read_data_from_postgres(spark, "w01_working_day", schema_name)

        # Get the max working day
        max_working_day = df_w01.agg(
            F.max("working_day").alias("working_day")
        ).collect()[0]["working_day"]

        # First MERGE operation: Join t22 and t20 to match the logic in the stored procedure
        df_t22_filtered = df_t22.alias("t22").join(
            df_t20.alias("t20"),
            (col("t22.working_day") == col("t20.working_day"))
            & (col("t22.account_currency") == col("t20.currency")),
        )
        df_t22_filtered = df_t22_filtered.filter(
            col("t22.working_day") == max_working_day
        )

        df_merged = df_t17.alias("t17").join(
            df_t22_filtered.alias("t22_filtered"),
            col("t17.id_product") == col("t22_filtered.id_product"),
            "left",
        )

        df_merged = df_merged.select(col("t17.*"), col("t22_filtered.EUR_RATE"))

        # Apply the function to get df with v_mindate
        df_with_v_mindate = func_date_since_pd_ho2_cc(spark, schema_name, df_merged)

        # Update date_since_pd_ho2
        df_result = df_with_v_mindate.withColumn(
            "date_since_pd_ho2",
            when(col("amount_past_due") > 0, col("v_mindate")).otherwise(
                col("date_since_pd_ho2")
            ),
        ).drop("v_mindate")

        # Update date_since_pd_ho2 in df_t17 directly
        df_t17 = (
            df_t17.alias("t17")
            .join(
                df_result.select(
                    col("id_product").alias("result_id_product"),
                    col("date_since_pd_ho2").alias("result_date_since_pd_ho2"),
                ).alias("result"),
                col("t17.id_product") == col("result_id_product"),
                "left_outer",
            )
            .withColumn(
                "date_since_pd_ho2",
                when(
                    col("result_date_since_pd_ho2").isNotNull(),
                    col("result_date_since_pd_ho2"),
                ).otherwise(col("date_since_pd_ho2")),
            )
            .drop("result_date_since_pd_ho2", "result_id_product")
        )  # Removing the joined columns

        # Removing aliases to avoid ambiguous references in the later joins
        df_t17 = df_t17.select(*[col(c).alias(c) for c in df_t17.columns])

        # Second MERGE operation: Update t17 based on customer table (t01)
        df_t17 = (
            df_t17.alias("t17")
            .join(
                df_t01.select(
                    col("nrp_customer").alias("t01_nrp_customer"),
                    col("dpd_ho2").alias("t01_dpd_ho2"),
                ).alias("t01"),
                col("t17.customer_number") == col("t01_nrp_customer"),
                "left_outer",
            )
            .withColumn(
                "dpd_ho2",
                when(
                    col("t17.retail_nonretail_indicator") == "NON-RETAIL",
                    col("t01_dpd_ho2"),
                ).otherwise(col("dpd_ho2")),
            )
            .drop("t01_nrp_customer", "t01_dpd_ho2")
        )  # Removing the joined columns

        # Removing aliases to avoid ambiguous references in the later joins
        df_t17 = df_t17.select(*[col(c).alias(c) for c in df_t17.columns])

        # FINAL update operation: Updating the dpd_ho2 values for Retail
        df_t17 = df_t17.withColumn(
            "dpd_ho2",
            when(
                (col("retail_nonretail_indicator") == "RETAIL")
                & col("date_since_pd_ho2").isNotNull(),
                round(datediff(col("working_day"), col("date_since_pd_ho2"))),
            ).otherwise(
                when(col("retail_nonretail_indicator") == "RETAIL", lit(0)).otherwise(
                    col("dpd_ho2")
                )
            ),
        )

        # Write to the final dataframe only the needed columns
        df_final = df_t17.select("id_product", "date_since_pd_ho2", "dpd_ho2")

        # Write the final dataframe to a temporary table
        df_final.write.jdbc(
            url=postgres_properties["url"],
            table='"ABACUS_A5"."temp_updated_rows"',
            mode="overwrite",  # Use overwrite for temporary table
            properties=postgres_properties,
        )

        update_query = """
        UPDATE "ABACUS_A5"."atmp_t17_dpd_credit_cards" AS t17
        SET date_since_pd_ho2 = temp.date_since_pd_ho2,
            dpd_ho2 = temp.dpd_ho2
        FROM "ABACUS_A5"."temp_updated_rows" AS temp
        WHERE t17.id_product = temp.id_product;
        """

        update_to_database(update_query)

        print("calculate_cc_dpd_ho2 completed successfully.")
    except Exception as e:
        print(f"Error in calculate_cc_dpd_ho2: {str(e)}")


if __name__ == "__main__":
    calculate_cc_dpd_ho2()
