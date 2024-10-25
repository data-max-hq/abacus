from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, concat, current_timestamp
from db_connection_util import (
    get_spark_session,
    get_postgres_properties,
    read_data_from_postgres,
    update_to_database,
)


def check_customer_product():
    try:
        spark = get_spark_session()
        postgres_properties = get_postgres_properties()
        schema_name = "ABACUS_A5"

        # Load necessary tables
        df_t02 = read_data_from_postgres(spark, "t02_customer_product", schema_name)
        df_t17 = read_data_from_postgres(
            spark, "atmp_t17_dpd_credit_cards", schema_name
        )
        df_t22 = read_data_from_postgres(spark, "t22_card_balance", schema_name)
        df_w01 = read_data_from_postgres(spark, "w01_working_day", schema_name)

        # Get max working day
        max_working_day = df_w01.agg(F.max("working_day")).collect()[0][0]

        # Filter t22_card_balance for max working day
        df_t22_filtered = df_t22.filter(
            col("working_day") == lit(max_working_day)
        ).select(
            col("id_product").alias("t22_id_product"),
            col("facility_id").alias("t22_facility_id"),
        )

        # Prepare the source dataframe
        df_source = (
            df_t17.select(
                col("id_product").alias("t17_id_product"),
                col("id_product_type").alias("t17_id_product_type"),
                col("days_past_due").alias("t17_days_past_due"),
                col("customer_number").alias("t17_customer_number"),
            )
            .join(
                df_t22_filtered, col("t17_id_product") == col("t22_id_product"), "left"
            )
            .select(
                col("t17_id_product").alias("source_id_product"),
                col("t17_id_product_type").alias("source_id_product_type"),
                lit("CREDIT CARD").alias("source_product_group"),
                col("t17_days_past_due").alias("source_days_past_due"),
                col("t17_customer_number").alias("source_customer_number"),
                concat(lit("CC"), col("t22_facility_id")).alias(
                    "source_ref_id_product"
                ),
            )
        )

        # Perform the merge operation
        df_merged = df_t02.select(
            col("id_product").alias("t02_id_product"),
            col("nrp_customer").alias("t02_nrp_customer"),
            col("id_product_type").alias("t02_id_product_type"),
            col("product_group").alias("t02_product_group"),
            col("check_flag").alias("t02_check_flag"),
            col("current_days_past_due").alias("t02_current_days_past_due"),
            col("ref_id_product").alias("t02_ref_id_product"),
            col("first_entry").alias("t02_first_entry"),
            col("last_update").alias("t02_last_update"),
        ).join(
            df_source, col("t02_id_product") == col("source_id_product"), "full_outer"
        )

        # Update existing records
        df_updates = df_merged.filter(col("t02_id_product").isNotNull()).select(
            col("t02_id_product").alias("id_product"),
            col("source_customer_number").alias("nrp_customer"),
            col("source_id_product_type").alias("id_product_type"),
            col("source_product_group").alias("product_group"),
            lit("1").alias("check_flag"),
            col("source_days_past_due").alias("current_days_past_due"),
            col("source_ref_id_product").alias("ref_id_product"),
            current_timestamp().alias("last_update"),
        )

        # Insert new records
        df_inserts = df_merged.filter(col("t02_id_product").isNull()).select(
            col("source_id_product").alias("id_product"),
            col("source_id_product_type").alias("id_product_type"),
            col("source_product_group").alias("product_group"),
            col("source_customer_number").alias("nrp_customer"),
            lit("1").alias("check_flag"),
            col("source_days_past_due").alias("current_days_past_due"),
            col("source_ref_id_product").alias("ref_id_product"),
            current_timestamp().alias("first_entry"),
            current_timestamp().alias("last_update"),
        )

        # Write updates to a temporary table
        df_updates.write.jdbc(
            url=postgres_properties["url"],
            table=f'"{schema_name}"."temp_t02_updates"',
            mode="overwrite",
            properties=postgres_properties,
        )

        # Update existing records
        update_query = f"""
            UPDATE "{schema_name}"."t02_customer_product" AS t02
            SET
                nrp_customer = temp.nrp_customer,
                id_product_type = temp.id_product_type,
                product_group = temp.product_group,
                check_flag = temp.check_flag,
                current_days_past_due = temp.current_days_past_due,
                ref_id_product = temp.ref_id_product,
                last_update = temp.last_update
            FROM "{schema_name}"."temp_t02_updates" AS temp
            WHERE t02.id_product = temp.id_product;
        """
        update_to_database(update_query)

        # Insert new records
        if df_inserts.count() > 0:
            df_inserts.write.jdbc(
                url=postgres_properties["url"],
                table=f'"{schema_name}"."t02_customer_product"',
                mode="append",
                properties=postgres_properties,
            )

        print("check_customer_product completed successfully.")
        return True
    except Exception as e:
        return False
        print(f"Error in check_customer_product: {str(e)}")


if __name__ == "__main__":
    check_customer_product()
