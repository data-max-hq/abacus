from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, lit, add_months, to_date, instr
from db_connection_util import (
    get_spark_session,
    get_postgres_properties,
    read_data_from_postgres,
    update_to_database,
)


def calc_cross_default_cc():
    try:
        spark = get_spark_session()
        postgres_properties = get_postgres_properties()
        schema_name = "ABACUS_A5"

        # Load necessary tables
        df_t17 = read_data_from_postgres(
            spark, "atmp_t17_dpd_credit_cards", schema_name
        )
        df_t22 = read_data_from_postgres(spark, "t22_card_balance", schema_name)
        df_t01 = read_data_from_postgres(spark, "t01_customer", schema_name)
        df_customer = read_data_from_postgres(spark, "atmp_customer", schema_name)
        df_w01 = read_data_from_postgres(spark, "w01_working_day", schema_name)
        df_w02 = read_data_from_postgres(spark, "w02_ccworking_day", schema_name)
        df_tc08 = read_data_from_postgres(
            spark, "tc08_configuration_table", schema_name
        )

        # Get the working day and next working day
        i_workingday = df_w01.agg(F.max("working_day")).collect()[0][0]
        v_next_working_day = df_w02.agg(F.max("working_day") + 1).collect()[0][0]

        # Cross default indicator from products of same product type
        tab_default = (
            df_t17.alias("tp")
            .join(
                df_t01.alias("tc"), col("tp.customer_number") == col("tc.nrp_customer")
            )
            .join(
                df_t22.alias("t22"),
                (col("tp.id_product") == col("t22.id_product"))
                & (col("t22.working_day") == lit(i_workingday)),
            )
            .filter(
                (col("tp.default_indicator") == "1")
                & (
                    ~col("tp.default_reason").isin(
                        "PRB CROSS DEFAULT PULLING EFFECT",
                        "CROSS DEFAULT PULLING EFFECT",
                    )
                )
                & (col("tc.customer_segment") == "PI")
                & (col("tp.account_code") != "1914000000")
            )
            .select(
                col("t22.customer_number").alias("tab_default_customer_number"),
                col("t22.secured_flag").alias("tab_default_secured_flag"),
                col("t22.working_day").alias("tab_default_working_day"),
            )
            .distinct()
        )

        cross_default_products = (
            df_t22.alias("t22")
            .join(
                tab_default.alias("tab_default"),
                (col("t22.customer_number") == col("tab_default_customer_number"))
                & (col("t22.secured_flag") == col("tab_default_secured_flag"))
                & (col("t22.working_day") == col("tab_default_working_day")),
            )
            .select(col("t22.id_product").alias("cross_default_id_product"))
            .distinct()
        )

        df_t17 = (
            df_t17.alias("t17")
            .join(
                cross_default_products.alias("cross_default"),
                col("t17.id_product") == col("cross_default_id_product"),
                "left",
            )
            .withColumn(
                "default_indicator",
                when(
                    (col("cross_default_id_product").isNotNull())
                    & (col("default_indicator") == "0")
                    & (col("account_code") != "1914000000"),
                    "1",
                ).otherwise(col("default_indicator")),
            )
            .withColumn(
                "default_contagion_indicator",
                when(
                    (col("cross_default_id_product").isNotNull())
                    & (col("default_indicator") == "0")
                    & (col("account_code") != "1914000000"),
                    "1",
                ).otherwise(col("default_contagion_indicator")),
            )
            .withColumn(
                "default_reason",
                when(
                    (col("cross_default_id_product").isNotNull())
                    & (col("default_indicator") == "0")
                    & (col("account_code") != "1914000000"),
                    when(col("dpd_ho") == 0, "PRB CROSS CONTAGION INDICATOR").otherwise(
                        "CROSS CONTAGION INDICATOR"
                    ),
                ).otherwise(col("default_reason")),
            )
            .withColumn(
                "proposed_default_ind",
                when(
                    (col("cross_default_id_product").isNotNull())
                    & (col("default_indicator") == "0")
                    & (col("account_code") != "1914000000"),
                    when(col("dpd_ho") == 0, "0").otherwise("1"),
                ).otherwise(col("proposed_default_ind")),
            )
            .withColumn(
                "is_probation",
                when(
                    (col("cross_default_id_product").isNotNull())
                    & (col("default_indicator") == "0")
                    & (col("account_code") != "1914000000"),
                    when(col("dpd_ho") == 0, "1").otherwise("0"),
                ).otherwise(col("is_probation")),
            )
            .withColumn(
                "start_probation_date",
                when(
                    (col("cross_default_id_product").isNotNull())
                    & (col("default_indicator") == "0")
                    & (col("account_code") != "1914000000"),
                    lit(v_next_working_day),
                ).otherwise(col("start_probation_date")),
            )
            .withColumn(
                "end_probation_date",
                when(
                    (col("cross_default_id_product").isNotNull())
                    & (col("default_indicator") == "0")
                    & (col("account_code") != "1914000000"),
                    add_months(lit(v_next_working_day), 3),
                ).otherwise(col("end_probation_date")),
            )
        )

        # Remove joined columns and aliases
        df_t17 = df_t17.drop("cross_default_id_product")
        df_t17 = df_t17.select(*[col(c).alias(c) for c in df_t17.columns])

        # Pulling effect for deals not defaulted previously
        # pulling_percentage = df_tc08.filter(col("SEGMENT_TYPE") == "RETAIL").select("PULLING_PERCENTAGE").first()[0] / 100
        pulling_percentage_row = (
            df_tc08.filter(col("SEGMENT_TYPE") == "RETAIL")
            .select("PULLING_PERCENTAGE")
            .first()
        )
        if pulling_percentage_row is not None:
            pulling_percentage = pulling_percentage_row[0] / 100
        else:
            print(
                "Warning: No PULLING_PERCENTAGE found for RETAIL segment. Using default value of 0.2"
            )
            pulling_percentage = 0.2

        df_customer_filtered = df_customer.filter(
            (col("customer_segment") == "PI")
            & (col("on_balance_default_eur") > 0)
            & (col("has_product") == "1")
            & (col("TOTAL_CREDIT_OBLIGATION") > col("on_balance_default_eur"))
            & (
                col("on_balance_default_eur")
                > lit(pulling_percentage) * col("TOTAL_CREDIT_OBLIGATION")
            )
        ).select(col("nrp_customer").alias("customer_filtered_nrp_customer"))

        df_t17 = (
            df_t17.alias("t17")
            .join(
                df_customer_filtered.alias("customer_filtered"),
                col("t17.customer_number") == col("customer_filtered_nrp_customer"),
                "left",
            )
            .withColumn(
                "default_indicator",
                when(
                    (col("customer_filtered_nrp_customer").isNotNull())
                    & (col("default_indicator") == "0")
                    & (col("account_code") != "1914000000"),
                    "1",
                ).otherwise(col("default_indicator")),
            )
            .withColumn(
                "default_contagion_indicator",
                when(
                    (col("customer_filtered_nrp_customer").isNotNull())
                    & (col("default_indicator") == "0")
                    & (col("account_code") != "1914000000"),
                    "1",
                ).otherwise(col("default_contagion_indicator")),
            )
            .withColumn(
                "default_reason",
                when(
                    (col("customer_filtered_nrp_customer").isNotNull())
                    & (col("default_indicator") == "0")
                    & (col("account_code") != "1914000000"),
                    when(
                        col("dpd_ho") == 0, "PRB CROSS DEFAULT PULLING EFFECT"
                    ).otherwise("CROSS DEFAULT PULLING EFFECT"),
                ).otherwise(col("default_reason")),
            )
            .withColumn(
                "proposed_default_ind",
                when(
                    (col("customer_filtered_nrp_customer").isNotNull())
                    & (col("default_indicator") == "0")
                    & (col("account_code") != "1914000000"),
                    when(col("dpd_ho") == 0, "0").otherwise("1"),
                ).otherwise(col("proposed_default_ind")),
            )
            .withColumn(
                "is_probation",
                when(
                    (col("customer_filtered_nrp_customer").isNotNull())
                    & (col("default_indicator") == "0")
                    & (col("account_code") != "1914000000"),
                    when(col("dpd_ho") == 0, "1").otherwise("0"),
                ).otherwise(col("is_probation")),
            )
            .withColumn(
                "start_probation_date",
                when(
                    (col("customer_filtered_nrp_customer").isNotNull())
                    & (col("default_indicator") == "0")
                    & (col("account_code") != "1914000000"),
                    lit(v_next_working_day),
                ).otherwise(col("start_probation_date")),
            )
            .withColumn(
                "end_probation_date",
                when(
                    (col("customer_filtered_nrp_customer").isNotNull())
                    & (col("default_indicator") == "0")
                    & (col("account_code") != "1914000000"),
                    add_months(lit(v_next_working_day), 3),
                ).otherwise(col("end_probation_date")),
            )
        )

        # Remove joined columns and aliases
        df_t17 = df_t17.drop("customer_filtered_nrp_customer")
        df_t17 = df_t17.select(*[col(c).alias(c) for c in df_t17.columns])

        # Reset probation period to 12 months for second default within a year
        df_t17 = df_t17.withColumn(
            "end_probation_date",
            when(
                (col("p_default_indicator") == "0")
                & (col("default_indicator") == "1")
                & (
                    add_months(col("working_day"), -12)
                    <= to_date(col("p_default_end_date"), "yyyyMMdd")
                )
                & (instr(col("default_reason"), "DECEASED") == 0),
                add_months(col("start_probation_date"), 12),
            )
            .when(
                (col("p_end_probation_date") > col("p_working_day"))
                & (col("p_end_probation_date") <= col("working_day")),
                col("end_probation_date"),
            )
            .when(
                (col("p_default_indicator") == "1")
                & (col("default_indicator") == "1")
                & (
                    add_months(to_date(col("p_default_end_date"), "yyyyMMdd"), 12)
                    >= to_date(col("p_default_start_date"), "yyyyMMdd")
                )
                & (instr(col("default_reason"), "DECEASED") == 0),
                add_months(col("start_probation_date"), 12),
            )
            .otherwise(col("end_probation_date")),
        ).filter(
            (col("retail_nonretail_indicator") == "RETAIL")
            & (col("default_indicator") == "1")
            & (col("p_default_end_date").isNotNull())
        )

        df_updated = df_t17.select(
            "id_product",
            "default_indicator",
            "default_contagion_indicator",
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
                    default_contagion_indicator = temp.default_contagion_indicator,
                    default_reason = temp.default_reason,
                    proposed_default_ind = temp.proposed_default_ind,
                    is_probation = temp.is_probation,
                    start_probation_date = temp.start_probation_date,
                    end_probation_date = temp.end_probation_date
                FROM "ABACUS_A5"."temp_updated_rows" AS temp
                WHERE t17.id_product = temp.id_product;
                """

        update_to_database(update_query)

        print("calculate_cross_default_cc completed successfully.")
    except Exception as e:
        print(f"Error in calculate_cross_default_cc: {str(e)}")


if __name__ == "__main__":
    calc_cross_default_cc()
