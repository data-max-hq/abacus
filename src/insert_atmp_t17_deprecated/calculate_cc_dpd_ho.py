from pyspark.sql import functions as F
from pyspark.sql.functions import col, round, when, lit, datediff, date_sub
from db_connection_util import (
    get_spark_session,
    get_postgres_properties,
    read_data_from_postgres,
    update_to_database,
)
from src.functions.func_cust_materiality_level import func_cust_materiality_level


def calculate_cc_dpd_ho():
    try:
        spark = get_spark_session()
        postgres_properties = get_postgres_properties()
        schema_name = "ABACUS_A5"

        # Read necessary tables
        df_t17 = read_data_from_postgres(
            spark, "atmp_t17_dpd_credit_cards", schema_name
        )
        df_t22 = read_data_from_postgres(spark, "t22_card_balance", schema_name)
        df_t20 = read_data_from_postgres(spark, "t20_exchange_rate", schema_name)
        df_w01 = read_data_from_postgres(spark, "w01_working_day", schema_name)
        df_atmp_joint_dpd = read_data_from_postgres(
            spark, "atmp_joint_dpd", schema_name
        )
        df_t01 = read_data_from_postgres(spark, "t01_customer", schema_name)
        df_tc08 = read_data_from_postgres(
            spark, "tc08_configuration_table", schema_name
        )

        # Get the max working day
        max_working_day = df_w01.agg(
            F.max("working_day").alias("working_day")
        ).collect()[0]["working_day"]

        # First MERGE operation with df_22_filtered
        df_t22_filtered = df_t22.alias("t22").join(
            df_t20.alias("t20"),
            (col("t22.working_day") == col("t20.working_day"))
            & (col("t22.account_currency") == col("t20.currency")),
        )

        df_t22_filtered = df_t22_filtered.filter(
            col("t22.working_day") == max_working_day
        )

        df_t22_filtered = df_t22_filtered.select(
            col("t22.id_product").alias("t22_id_product"),
            col("t22.ledger_balance").alias("t22_ledger_balance"),
            col("t20.EUR_RATE").alias("t22_EUR_RATE"),
            col("t22.working_day").alias("t22_working_day"),
            col("t22.ACCOUNT_CURRENCY").alias("t22_ACCOUNT_CURRENCY"),
        )

        df_merged = df_t17.alias("t17").join(
            df_t22_filtered.alias("t22"),
            df_t17.id_product == df_t22_filtered.t22_id_product,
            "left",
        )

        # Apply the materiality function
        df_merged = func_cust_materiality_level(df_merged, df_tc08)

        # Keeping only the columns of atmp_t17_dpd_credit_cards to avoid ambiguity
        df_merged = df_merged.drop(
            "t22_id_product",
            "t22_ledger_balance",
            "t22_EUR_RATE",
            "t22_working_day",
            "t22_ACCOUNT_CURRENCY",
            "segment_type",
            "pd_threshold",
            "percentage_threshold",
            "dpd_threshold",
            "retention_year",
            "pulling_percentage",
            "pd_threshold2",
            "max_debtor_dpd_forbearance",
            "percentage_threshold2",
        )

        # Second MERGE operation with t20
        df_t20_max = df_t20.filter(df_t20.working_day == max_working_day)

        df_t20_max = df_t20_max.select(
            col("working_day").alias("t20_working_day"),
            col("EUR_RATE").alias("t20_EUR_RATE"),
            col("currency").alias("t20_currency"),
        )

        df_merged = df_merged.join(
            df_t20_max,
            (col("card_ccy") == col("t20_currency"))
            & (col("t20_working_day") == lit(max_working_day)),
            "left",
        )

        df_merged = df_merged.withColumn(
            "dpd_ho",
            when(
                col("retail_nonretail_indicator") == "RETAIL",
                when(
                    (
                        col("amount_past_due") * col("t20_EUR_RATE")
                        > col("materiality_level")
                    )
                    & (col("p_dpd_ho") == 0),
                    1,
                )
                .when(
                    (
                        col("amount_past_due") * col("t20_EUR_RATE")
                        > col("materiality_level")
                    )
                    & (col("p_dpd_ho") > 0),
                    col("p_dpd_ho")
                    + round(datediff(col("t20_working_day"), col("p_working_day"))),
                )
                .when(
                    (
                        col("amount_past_due") * col("t20_EUR_RATE")
                        <= col("materiality_level")
                    )
                    & (col("amount_past_due") > 0)
                    & (col("p_dpd_ho") > 0)
                    & (col("amount_past_due") >= col("p_amount_past_due")),
                    col("p_dpd_ho")
                    + round(datediff(col("t20_working_day"), col("p_working_day"))),
                )
                .otherwise(0),
            ).otherwise(col("dpd_ho")),
        )

        # Keeping only the columns of atmp_t17_dpd_credit_cards
        df_merged = df_merged.drop("t20_working_day", "t20_EUR_RATE", "t20_currency")

        # Update for joint products
        df_joint = df_merged.join(
            df_atmp_joint_dpd.select(
                col("joint_id").alias("joint_joint_id"),
                col("dpd_ho").alias("joint_dpd_ho"),
            ),
            col("joint_id") == col("joint_joint_id"),
            "left",
        )

        df_joint = df_joint.withColumn(
            "dpd_ho",
            when(
                (col("retail_nonretail_indicator") == "RETAIL")
                & (col("is_joint") == "1"),
                col("joint_dpd_ho"),
            ).otherwise(col("dpd_ho")),
        )

        # Update for NONRETAIL
        df_nonretail = df_joint.join(
            df_t01.select(
                col("nrp_customer").alias("t01_nrp_customer"),
                col("dpd_ho").alias("t01_dpd_ho"),
            ),
            col("customer_number") == col("t01_nrp_customer"),
            "left",
        )
        df_nonretail = df_nonretail.withColumn(
            "dpd_ho",
            when(
                (col("retail_nonretail_indicator") == "NONRETAIL")
                & (col("t01_dpd_ho").isNotNull()),
                col("t01_dpd_ho"),
            ).otherwise(col("dpd_ho")),
        )

        # Final UPDATE
        df_final = df_nonretail.withColumn(
            "date_since_pd_ho",
            when(
                (col("dpd_ho") > 0) & (col("p_date_since_pd_ho").isNotNull()),
                date_sub(col("working_day"), col("dpd_ho").cast("int")),
            )
            .when(
                (col("dpd_ho") > 0) & (col("p_date_since_pd_ho").isNull()),
                col("working_day"),
            )
            .otherwise(None),
        )

        # Write to the final dataframe only the needed columns
        df_final = df_final.select(
            "id_product", "date_since_pd_ho", "dpd_ho", "materiality_level"
        )

        # Write the final dataframe to a temporary table
        df_final.write.jdbc(
            url=postgres_properties["url"],
            table='"ABACUS_A5"."temp_updated_rows"',
            mode="overwrite",  # Use overwrite for temporary table
            properties=postgres_properties,
        )

        update_query = """
                UPDATE "ABACUS_A5"."atmp_t17_dpd_credit_cards" AS t17
                SET date_since_pd_ho = temp.date_since_pd_ho,
                    dpd_ho = temp.dpd_ho,
                    materiality_level = temp.materiality_level
                FROM "ABACUS_A5"."temp_updated_rows" AS temp
                WHERE t17.id_product = temp.id_product;
                """

        update_to_database(update_query)

        print("calculate_cc_dpd_ho completed succesfully.")
    except Exception as e:
        print(f"Error in calculate_cc_dpd_ho: {str(e)}")


if __name__ == "__main__":
    calculate_cc_dpd_ho()
