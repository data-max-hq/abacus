from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, lit, concat, abs, coalesce
from pyspark.sql.types import IntegerType
from db_connection_util import (
    get_spark_session,
    get_postgres_properties,
    read_data_from_postgres,
    update_to_database,
)


def calc_default_fields_cc():
    try:
        spark = get_spark_session()
        postgres_properties = get_postgres_properties()
        schema_name = "ABACUS_A5"

        # Load necessary tables
        df_t17 = read_data_from_postgres(
            spark, "atmp_t17_dpd_credit_cards", schema_name
        )
        df_t22 = read_data_from_postgres(spark, "t22_card_balance", schema_name)
        df_customer = read_data_from_postgres(spark, "atmp_customer", schema_name)
        df_w01 = read_data_from_postgres(spark, "w01_working_day", schema_name)

        # Get the working day
        i_workingday = df_w01.agg(F.max("working_day")).collect()[0][0]

        # Historical default indicator
        df_t17 = df_t17.withColumn(
            "historical_default_indicator",
            when(
                (col("p_historical_default_indicator") == "0")
                & (col("default_indicator") == "1"),
                "1",
            ).otherwise(coalesce(col("p_historical_default_indicator"), lit("0"))),
        )

        # Default amount
        df_t17 = df_t17.join(
            df_t22.filter(col("working_day") == lit(i_workingday)).select(
                col("id_product").alias("t22_id_product"),
                col("ledger_balance").alias("t22_ledger_balance"),
            ),
            df_t17.id_product == col("t22_id_product"),
            "left",
        ).withColumn(
            "default_amount",
            when(
                (col("p_default_indicator") == "0") & (col("default_indicator") == "1"),
                when(
                    col("t22_ledger_balance") <= 0, abs(col("t22_ledger_balance"))
                ).otherwise(0),
            )
            .when(
                (col("p_default_indicator") == "1") & (col("default_indicator") == "1"),
                coalesce(col("p_default_amount"), lit(0)),
            )
            .otherwise(0),
        )

        # Default start date
        df_t17 = df_t17.withColumn(
            "default_start_date",
            when(
                (col("p_default_indicator") == "0") & (col("default_indicator") == "1"),
                F.date_format(F.date_sub(col("working_day"), 1), "yyyyMMdd").cast(
                    IntegerType()
                ),
            )
            .when(
                (col("p_default_indicator") == "1") & (col("default_indicator") == "1"),
                coalesce(
                    col("p_default_start_date"),
                    F.date_format(F.date_sub(col("working_day"), 1), "yyyyMMdd").cast(
                        IntegerType()
                    ),
                ),
            )
            .otherwise(None),
        ).filter(col("RETAIL_NONRETAIL_INDICATOR") == "RETAIL")

        # Remove joined columns and aliases
        df_t17 = df_t17.drop("t22_id_product", "t22_ledger_balance")
        df_t17 = df_t17.select(*[col(c).alias(c) for c in df_t17.columns])

        # Update default start date for NONRETAIL
        df_t17 = df_t17.join(
            df_customer.select(
                col("nrp_customer").alias("cust_nrp_customer"),
                col("default_start_date").alias("cust_default_start_date"),
            ),
            df_t17.customer_number == col("cust_nrp_customer"),
            "left",
        ).withColumn(
            "default_start_date",
            when(
                col("RETAIL_NONRETAIL_INDICATOR") == "NONRETAIL",
                F.date_format(col("cust_default_start_date"), "yyyyMMdd").cast(
                    IntegerType()
                ),
            ).otherwise(col("default_start_date")),
        )

        # Default end date
        df_t17 = df_t17.withColumn(
            "default_end_date",
            when(
                (col("p_default_indicator") == "1") & (col("default_indicator") == "0"),
                F.date_format(F.date_sub(col("working_day"), 1), "yyyyMMdd").cast(
                    IntegerType()
                ),
            ).otherwise(col("p_default_end_date")),
        )

        # Default ID
        df_t17 = df_t17.withColumn(
            "default_id",
            when(
                (col("p_default_indicator") == "0") & (col("default_indicator") == "1"),
                concat(
                    col("customer_number"), col("id_product"), col("default_start_date")
                ),
            )
            .when(col("default_indicator") == "0", None)
            .otherwise(
                coalesce(
                    col("default_id"),
                    concat(
                        col("customer_number"),
                        col("id_product"),
                        col("default_start_date"),
                    ),
                )
            ),
        )

        # Interest rate default
        df_t17 = df_t17.withColumn(
            "interest_rate_default",
            when(
                (col("p_default_indicator") == "0") & (col("default_indicator") == "1"),
                col("standart_interest_rate"),
            )
            .when(col("default_indicator") == "0", 0)
            .otherwise(
                coalesce(col("interest_rate_default"), col("standart_interest_rate"))
            ),
        )

        # Remove joined columns and aliases
        df_t17 = df_t17.drop("cust_nrp_customer", "cust_default_start_date")
        df_t17 = df_t17.select(*[col(c).alias(c) for c in df_t17.columns])

        df_updated = df_t17.select(
            "id_product",
            "historical_default_indicator",
            "default_amount",
            "default_start_date",
            "default_end_date",
            "default_id",
            "interest_rate_default",
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
                    historical_default_indicator = temp.historical_default_indicator,
                    default_amount = temp.default_amount,
                    default_start_date = temp.default_start_date,
                    default_end_date = temp.default_end_date,
                    default_id = temp.default_id,
                    interest_rate_default = temp.interest_rate_default
                FROM "ABACUS_A5"."temp_updated_rows" AS temp
                WHERE t17.id_product = temp.id_product;
                """

        update_to_database(update_query)

        print("calculate_default_fields_cc completed successfully.")
    except Exception as e:
        print(f"Error in calculate_default_fields_cc: {str(e)}")


if __name__ == "__main__":
    calc_default_fields_cc()
