from pyspark.pandas import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, lit, abs

from db_connection_util import (
    get_spark_session,
    get_postgres_properties,
    read_data_from_postgres,
    update_to_database,
)
from src.core.structured_etl import StructuredEtl

DPD_CREDIT_CARDS_TABLE = "atmp_t17_dpd_credit_cards"
CARD_BALANCE_TABLE = "t22_card_balance"
WORKING_DAY_TABLE = "w01_working_day"
CC_WORKING_DAY_TABLE = "w02_ccworking_day"

class GcsEtl(StructuredEtl):
    def __init__(self):
        self.postgres_properties = get_postgres_properties()

    def read(self):
        spark = get_spark_session()
        schema_name = "ABACUS_A5"

        # Load necessary tables
        dpd_credit_cards_df = read_data_from_postgres(
            spark, DPD_CREDIT_CARDS_TABLE, schema_name
        )
        card_balance_df = read_data_from_postgres(
            spark, CARD_BALANCE_TABLE, schema_name
        )
        working_day_df = read_data_from_postgres(spark, WORKING_DAY_TABLE, schema_name)
        cc_working_day_df = read_data_from_postgres(
            spark, CC_WORKING_DAY_TABLE, schema_name
        )
        return dpd_credit_cards_df, card_balance_df, working_day_df, cc_working_day_df

    def transform(
        self, dpd_credit_cards_df, card_balance_df, working_day_df, cc_working_day_df
    ) -> tuple[DataFrame, ...]:
        max_cc_working_day = cc_working_day_df.agg(F.max("working_day")).collect()[0][0]
        max_working_day = (
            working_day_df.filter(col("working_day") <= lit(max_cc_working_day))
            .agg(F.max("working_day"))
            .collect()[0][0]
        )

        # Filter t22_card_balance for the max working day and select only necessary columns with aliases
        card_balance_filtered_df = card_balance_df.filter(
            col("working_day") == lit(max_working_day)
        ).select(
            col("id_product").alias("t22_id_product"),
            col("ledger_balance").alias("t22_ledger_balance"),
            col("account_code").alias("t22_account_code"),
            col("unamortized_fee").alias("t22_unamortized_fee"),
        )

        # Perform the merge operation
        dpd_credit_cards_df = dpd_credit_cards_df.join(
            card_balance_filtered_df, col("id_product") == col("t22_id_product"), "left"
        ).withColumn(
            "gca",
            when(
                (col("t22_ledger_balance") >= 0)
                | (col("t22_account_code") == "1914000000"),
                lit(0),
            ).otherwise(abs(col("t22_ledger_balance")) - col("t22_unamortized_fee")),
        )

        # Update gca to 0 if it's negative
        dpd_credit_cards_df = dpd_credit_cards_df.withColumn(
            "gca", when(col("gca") < 0, lit(0)).otherwise(col("gca"))
        )

        # Remove joined columns and aliases.
        dpd_credit_cards_df = dpd_credit_cards_df.drop(
            "t22_id_product",
            "t22_ledger_balance",
            "t22_account_code",
            "t22_unamortized_fee",
        )
        dpd_credit_cards_df = dpd_credit_cards_df.select(
            *[col(c).alias(c) for c in dpd_credit_cards_df.columns]
        )
        df_updated = dpd_credit_cards_df.select("id_product", "gca")
        return (df_updated,)

    def persist(self, df_updated) -> None:
        df_updated.write.jdbc(
            url=self.postgres_properties["url"],
            table='"ABACUS_A5"."temp_updated_rows"',
            mode="overwrite",  # Use overwrite for temporary table
            properties=self.postgres_properties,
        )

        update_query = """
                       UPDATE "ABACUS_A5"."DPD_CREDIT_CARDS_TABLE" AS t17
                       SET gca = temp.gca
                       FROM "ABACUS_A5"."temp_updated_rows" AS temp
                       WHERE t17.id_product = temp.id_product;
                       """

        update_to_database(update_query)

if __name__ == "__main__":
    try:
        print("GcsEtl processing started ...")
        gsc_data_processor = GcsEtl()
        df_t17, df_t22, df_w01, df_w02 = gsc_data_processor.read()
        result = gsc_data_processor.transform(df_t17, df_t22, df_w01, df_w02)
        gsc_data_processor.persist(result[0])
        print("GcsEtl processing completed successfully")

    except Exception as e:
        print(f"GcsEtl processing failed with error: {str(e)}")
