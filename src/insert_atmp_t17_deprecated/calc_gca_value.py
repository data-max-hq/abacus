from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, lit, abs
from db_connection_util import (
    get_spark_session,
    get_postgres_properties,
    read_data_from_postgres,
    update_to_database,
)


def calc_gca_value():
    try:
        spark = get_spark_session()
        postgres_properties = get_postgres_properties()
        schema_name = "ABACUS_A5"

        # Load necessary tables
        df_t17 = read_data_from_postgres(
            spark, "atmp_t17_dpd_credit_cards", schema_name
        )
        df_t22 = read_data_from_postgres(spark, "t22_card_balance", schema_name)
        df_w01 = read_data_from_postgres(spark, "w01_working_day", schema_name)
        df_w02 = read_data_from_postgres(spark, "w02_ccworking_day", schema_name)

        # Get the max working day
        max_cc_working_day = df_w02.agg(F.max("working_day")).collect()[0][0]
        max_working_day = (
            df_w01.filter(col("working_day") <= lit(max_cc_working_day))
            .agg(F.max("working_day"))
            .collect()[0][0]
        )

        # Filter t22_card_balance for the max working day and select only necessary columns with aliases
        df_t22_filtered = df_t22.filter(
            col("working_day") == lit(max_working_day)
        ).select(
            col("id_product").alias("t22_id_product"),
            col("ledger_balance").alias("t22_ledger_balance"),
            col("account_code").alias("t22_account_code"),
            col("unamortized_fee").alias("t22_unamortized_fee"),
        )

        # Perform the merge operation
        df_t17 = df_t17.join(
            df_t22_filtered, col("id_product") == col("t22_id_product"), "left"
        ).withColumn(
            "gca",
            when(
                (col("t22_ledger_balance") >= 0)
                | (col("t22_account_code") == "1914000000"),
                lit(0),
            ).otherwise(abs(col("t22_ledger_balance")) - col("t22_unamortized_fee")),
        )

        # Update gca to 0 if it's negative
        df_t17 = df_t17.withColumn(
            "gca", when(col("gca") < 0, lit(0)).otherwise(col("gca"))
        )

        # Remove joined columns and aliases.
        df_t17 = df_t17.drop(
            "t22_id_product",
            "t22_ledger_balance",
            "t22_account_code",
            "t22_unamortized_fee",
        )
        df_t17 = df_t17.select(*[col(c).alias(c) for c in df_t17.columns])

        df_updated = df_t17.select("id_product", "gca")

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
                    gca = temp.gca
                FROM "ABACUS_A5"."temp_updated_rows" AS temp
                WHERE t17.id_product = temp.id_product;
                """

        update_to_database(update_query)

        print("calculate_gca_value completed successfully.")
    except Exception as e:
        print(f"Error in calculate_gca_value: {str(e)}")


if __name__ == "__main__":
    calc_gca_value()
