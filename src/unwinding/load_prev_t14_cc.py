from pyspark.sql.functions import col, when, max as max_
from db_connection_util import (
    get_spark_session,
    get_postgres_properties,
    read_data_from_postgres,
    update_to_database,
)


def load_prev_t14_cc():
    try:
        spark = get_spark_session()
        postgres_properties = get_postgres_properties()
        schema_name = "ABACUS_A5"

        # Load necessary tables
        df_t14 = read_data_from_postgres(spark, "t14_unwinding", schema_name)
        df_w02 = read_data_from_postgres(spark, "w02_ccworking_day", schema_name)

        # Get the max working day for credit cards
        max_working_day = df_w02.agg(max_("working_day")).collect()[0][0]

        # Get the previous max working day for credit cards
        prev_max_working_day = (
            df_w02.filter(col("working_day") < max_working_day)
            .agg(max_("working_day"))
            .collect()[0][0]
        )

        # Filter t14_unwinding for the current max working day (target rows to update)
        df_t14_current = df_t14.filter(
            (col("working_day") == max_working_day) & (col("product_group") == "CC")
        )

        # Filter t14_unwinding for the previous max working day (source rows for update)
        df_t14_prev = df_t14.filter(col("working_day") == prev_max_working_day)

        # Perform the merge operation
        df_updated = (
            df_t14_current.alias("nrm")
            .join(
                df_t14_prev.alias("src"),
                col("nrm.id_product") == col("src.id_product"),
                "left",
            )
            .select(
                "nrm.*",
                when(col("src.id_product").isNotNull(), col("src.unwinding_mtd_lcy"))
                .otherwise(col("nrm.unwinding_mtd_lcy"))
                .alias("new_unwinding_mtd_lcy"),
            )
        )

        # Update the unwinding_mtd_lcy column
        df_updated = df_updated.withColumn(
            "unwinding_mtd_lcy", col("new_unwinding_mtd_lcy")
        ).drop("new_unwinding_mtd_lcy")

        df_updated = df_updated.select(
            "autoid",
            "unwinding_mtd_lcy",
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
                                unwinding_mtd_lcy = temp.unwinding_mtd_lcy
                            FROM "ABACUS_A5"."temp_updated_rows" AS temp
                            WHERE t14.autoid = temp.autoid;
                            """

        update_to_database(update_query)

        print("load_prev_t14_cc completed successfully.")
    except Exception as e:
        print(f"Error in load_prev_t14_cc: {str(e)}")


if __name__ == "__main__":
    load_prev_t14_cc()
