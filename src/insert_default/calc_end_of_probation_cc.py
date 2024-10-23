from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, lit
from db_connection_util import (
    get_spark_session,
    get_postgres_properties,
    read_data_from_postgres,
    update_to_database,
)


def calc_end_of_probation_cc():
    try:
        spark = get_spark_session()
        postgres_properties = get_postgres_properties()
        schema_name = "ABACUS_A5"

        # Load necessary tables
        df_t17 = read_data_from_postgres(
            spark, "atmp_t17_dpd_credit_cards", schema_name
        )
        df_customer = read_data_from_postgres(spark, "atmp_customer", schema_name)
        df_w02 = read_data_from_postgres(spark, "w02_ccworking_day", schema_name)

        # Get the next working day
        v_next_working_day = df_w02.agg(F.max("working_day") + 1).collect()[0][0]

        # Prepare the customer dataframe
        df_customer = df_customer.filter(
            (col("retail_nonretail") == "RETAIL") & (col("has_product") == "1")
        )

        # Merge operation
        df_t17 = df_t17.alias("t17").join(
            df_customer.select(
                col("nrp_customer").alias("cust_nrp_customer"),
                col("max_dpd").alias("cust_max_dpd"),
                col("IS_FORBEARANCE_LAST3M").alias("cust_IS_FORBEARANCE_LAST3M"),
                col("NO_BASE_DEFAULTS").alias("cust_NO_BASE_DEFAULTS"),
            ).alias("cust"),
            col("t17.customer_number") == col("cust_nrp_customer"),
            "left",
        )

        df_t17 = (
            df_t17.withColumn(
                "default_indicator",
                when(
                    (col("cust_max_dpd") < 60)
                    & (col("cust_IS_FORBEARANCE_LAST3M") == "0")
                    & (col("cust_NO_BASE_DEFAULTS") == "0"),
                    "0",
                ).otherwise("1"),
            )
            .withColumn(
                "default_reason",
                when(
                    (col("cust_max_dpd") < 60)
                    & (col("cust_IS_FORBEARANCE_LAST3M") == "0")
                    & (col("cust_NO_BASE_DEFAULTS") == "0"),
                    "",
                ).otherwise(col("P_DEFAULT_REASON")),
            )
            .withColumn("proposed_default_ind", lit(0))
            .withColumn(
                "is_probation",
                when(
                    (col("cust_max_dpd") < 60)
                    & (col("cust_IS_FORBEARANCE_LAST3M") == "0")
                    & (col("cust_NO_BASE_DEFAULTS") == "0"),
                    "0",
                ).otherwise("1"),
            )
            .withColumn(
                "start_probation_date",
                when(
                    (col("cust_max_dpd") < 60)
                    & (col("cust_IS_FORBEARANCE_LAST3M") == "0")
                    & (col("cust_NO_BASE_DEFAULTS") == "0"),
                    None,
                ).otherwise(col("P_start_probation_date")),
            )
            .withColumn(
                "end_probation_date",
                when(
                    (col("cust_max_dpd") < 60)
                    & (col("cust_IS_FORBEARANCE_LAST3M") == "0")
                    & (col("cust_NO_BASE_DEFAULTS") == "0"),
                    None,
                ).otherwise(lit(v_next_working_day)),
            )
            .filter(
                (col("end_probation_date") > col("p_working_day"))
                & (col("end_probation_date") <= col("working_day"))
                & (col("p_is_probation") == 1)
                & (col("is_probation") == 1)
            )
        )

        # Remove joined columns and aliases
        df_t17 = df_t17.drop(
            "cust_nrp_customer",
            "cust_max_dpd",
            "cust_IS_FORBEARANCE_LAST3M",
            "cust_NO_BASE_DEFAULTS",
        )
        df_t17 = df_t17.select(*[col(c).alias(c) for c in df_t17.columns])

        df_updated = df_t17.select(
            "id_product",
            "default_indicator",
            "default_reason",
            "proposed_default_ind",
            "is_probation",
            "start_probation_date",
            "end_probation_date",
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
                    default_indicator = temp.default_indicator,
                    default_reason = temp.default_reason,
                    proposed_default_ind = temp.proposed_default_ind,
                    is_probation = temp.is_probation,
                    start_probation_date = temp.start_probation_date,
                    end_probation_date = temp.end_probation_date
                FROM "ABACUS_A5"."temp_updated_rows" AS temp
                WHERE t17.id_product = temp.id_product;
                """

        update_to_database(update_query)

        print("calculate_end_of_probation_cc completed successfully.")
    except Exception as e:
        print(f"Error in calculate_end_of_probation_cc: {str(e)}")


if __name__ == "__main__":
    calc_end_of_probation_cc()
