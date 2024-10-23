from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, coalesce, max as max_
from pyspark.sql.types import DecimalType
from db_connection_util import (
    get_spark_session,
    get_postgres_properties,
    read_data_from_postgres,
)


def insert_t14_unwinding_cc():
    try:
        spark = get_spark_session()
        postgres_properties = get_postgres_properties()
        schema_name = "ABACUS_A5"

        # Load necessary tables
        df_t14 = read_data_from_postgres(spark, "t14_unwinding", schema_name)
        df_t17 = read_data_from_postgres(
            spark, "atmp_t17_dpd_credit_cards", schema_name
        )
        df_t22 = read_data_from_postgres(spark, "t22_card_balance", schema_name)
        df_t20 = read_data_from_postgres(spark, "t20_exchange_rate", schema_name)
        df_vw_unwinding = read_data_from_postgres(spark, "VW_UNWINDING", schema_name)
        df_w01 = read_data_from_postgres(spark, "w01_working_day", schema_name)
        df_w02 = read_data_from_postgres(spark, "w02_ccworking_day", schema_name)

        # Get the max working day for credit cards
        v_working_day = df_w02.agg(max_("working_day")).collect()[0][0]

        # Get the max working day for the join condition
        max_w01_working_day = (
            df_w01.filter(col("working_day") <= v_working_day)
            .agg(max_("working_day"))
            .collect()[0][0]
        )

        # Delete existing records
        df_t14 = df_t14.filter(
            (col("working_day") != v_working_day) | (col("product_group") != "CC")
        )

        # Prepare the data for insertion
        df_insert = (
            df_t17.alias("t17")
            .join(
                df_t22.alias("t22"),
                (col("t17.id_product") == col("t22.id_product"))
                & (col("t22.working_day") == lit(max_w01_working_day)),
                "inner",
            )
            .join(
                df_t20.alias("t20"),
                (col("t22.working_day") == col("t20.working_day"))
                & (col("t22.account_currency") == col("t20.currency")),
                "inner",
            )
            .join(
                df_vw_unwinding.alias("t01"),
                col("t17.id_product") == col("t01.id_product"),
                "left",
            )
            .select(
                F.monotonically_increasing_id().alias("autoid"),
                col("t17.working_day"),
                col("t17.id_product"),
                col("facility_id"),
                col("t17.account_number"),
                col("t17.p_working_day"),
                col("t17.working_day").alias("closing_day"),
                col("t17.p_working_day").alias("p_closing_day"),
                lit("CC").alias("product_group"),
                col("t17.retail_nonretail_indicator").alias("retail_nonretail"),
                (col("t17.gca") * col("t20.all_rate")).alias("gca_all"),
                col("t22.calculation_basis"),
                col("t17.standart_interest_rate").alias("interest_rate"),
                coalesce(col("t01.overwritten_ecl"), lit(0)).alias("provision_fund"),
                lit(0).cast(DecimalType(22, 2)).alias("unwinding_daily_lcy"),
                lit(0).cast(DecimalType(22, 2)).alias("unwinding_mtd_lcy"),
                lit(0).cast(DecimalType(22, 2)).alias("unwinding_daily_ocy"),
                lit(0).cast(DecimalType(22, 2)).alias("unwinding_mtd_ocy"),
                col("t17.account_currency").alias("deal_ccy"),
            )
        )

        # Ensure df_insert has the same schema as df_t14
        for column in df_t14.columns:
            if column not in df_insert.columns:
                df_insert = df_insert.withColumn(
                    column, lit(None).cast(df_t14.schema[column].dataType)
                )

        # Write only the new unwinding data to the database
        df_insert.write.jdbc(
            url=postgres_properties["url"],
            table='"ABACUS_A5"."t14_unwinding"',
            mode="append",
            properties=postgres_properties,
        )

        print("insert_t14_unwinding_cc completed successfully.")
    except Exception as e:
        print(f"Error in insert_t14_unwinding_cc: {str(e)}")


if __name__ == "__main__":
    insert_t14_unwinding_cc()
