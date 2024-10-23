from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, lit, add_months
from db_connection_util import (
    get_spark_session,
    get_postgres_properties,
    read_data_from_postgres,
    update_to_database,
)


def calc_probation_cc():
    try:
        spark = get_spark_session()
        postgres_properties = get_postgres_properties()
        schema_name = "ABACUS_A5"

        # Load necessary tables
        df_t17 = read_data_from_postgres(
            spark, "atmp_t17_dpd_credit_cards", schema_name
        )
        df_t01 = read_data_from_postgres(spark, "atmp_customer", schema_name)
        df_w02 = read_data_from_postgres(spark, "w02_ccworking_day", schema_name)

        # Get the max working day + 1
        i_ccworkday = df_w02.agg(F.max("working_day") + 1).collect()[0][0]

        # First MERGE operation: Update for RETAIL customers
        df_t01_retail = df_t01.filter(
            (col("retail_nonretail") == "RETAIL") & (col("has_product") == "1")
        )
        df_t17 = df_t17.alias("t17").join(
            df_t01_retail.select(
                col("nrp_customer").alias("t01_nrp_customer"),
                col("max_dpd").alias("t01_max_dpd"),
                col("customer_segment").alias("t01_customer_segment"),
            ).alias("t01"),
            col("t17.customer_number") == col("t01_nrp_customer"),
            "left",
        )

        df_t17 = (
            df_t17.withColumn(
                "default_indicator",
                when(
                    (
                        col("p_default_reason").like("%BANKRUPTACY%")
                        | col("p_default_reason").like("%DECEASED%")
                        | col("p_default_reason").like("%LOSS OF SUFFICIENT INCOME%")
                        | col("p_default_reason").like("%INDEBTEDNESS%")
                    )
                    & (col("p_is_probation") == 0)
                    & (col("proposed_default_ind") == "0")
                    & (col("p_proposed_default_ind") == "1"),
                    lit("1"),
                ).otherwise(col("default_indicator")),
            )
            .withColumn(
                "proposed_default_ind",
                when(
                    (
                        col("p_default_reason").like("%BANKRUPTACY%")
                        | col("p_default_reason").like("%DECEASED%")
                        | col("p_default_reason").like("%LOSS OF SUFFICIENT INCOME%")
                        | col("p_default_reason").like("%INDEBTEDNESS%")
                    )
                    & (col("p_is_probation") == 0)
                    & (col("proposed_default_ind") == "0")
                    & (col("p_proposed_default_ind") == "1"),
                    when(col("t01_max_dpd") > 0, "1").otherwise("0"),
                ).otherwise(col("proposed_default_ind")),
            )
            .withColumn(
                "default_reason",
                when(
                    (
                        col("p_default_reason").like("%BANKRUPTACY%")
                        | col("p_default_reason").like("%DECEASED%")
                        | col("p_default_reason").like("%LOSS OF SUFFICIENT INCOME%")
                        | col("p_default_reason").like("%INDEBTEDNESS%")
                    )
                    & (col("p_is_probation") == 0)
                    & (col("proposed_default_ind") == "0")
                    & (col("p_proposed_default_ind") == "1"),
                    when(col("t01_max_dpd") > 0, col("p_default_reason")).otherwise(
                        F.concat(lit("PRB "), col("p_default_reason"))
                    ),
                ).otherwise(col("default_reason")),
            )
            .withColumn(
                "is_probation",
                when(
                    (
                        col("p_default_reason").like("%BANKRUPTACY%")
                        | col("p_default_reason").like("%DECEASED%")
                        | col("p_default_reason").like("%LOSS OF SUFFICIENT INCOME%")
                        | col("p_default_reason").like("%INDEBTEDNESS%")
                    )
                    & (col("p_is_probation") == 0)
                    & (col("proposed_default_ind") == "0")
                    & (col("p_proposed_default_ind") == "1"),
                    when(col("t01_max_dpd") > 0, "0").otherwise("1"),
                ).otherwise(col("is_probation")),
            )
            .withColumn(
                "start_probation_date",
                when(
                    (
                        col("p_default_reason").like("%BANKRUPTACY%")
                        | col("p_default_reason").like("%DECEASED%")
                        | col("p_default_reason").like("%LOSS OF SUFFICIENT INCOME%")
                        | col("p_default_reason").like("%INDEBTEDNESS%")
                    )
                    & (col("p_is_probation") == 0)
                    & (col("proposed_default_ind") == "0")
                    & (col("p_proposed_default_ind") == "1"),
                    when(col("t01_max_dpd") > 0, lit(i_ccworkday)).otherwise(
                        col("p_start_probation_date")
                    ),
                ).otherwise(col("start_probation_date")),
            )
            .withColumn(
                "end_probation_date",
                when(
                    (
                        col("p_default_reason").like("%BANKRUPTACY%")
                        | col("p_default_reason").like("%DECEASED%")
                        | col("p_default_reason").like("%LOSS OF SUFFICIENT INCOME%")
                        | col("p_default_reason").like("%INDEBTEDNESS%")
                    )
                    & (col("p_is_probation") == 0)
                    & (col("proposed_default_ind") == "0")
                    & (col("p_proposed_default_ind") == "1"),
                    when(
                        col("t01_max_dpd") > 0, add_months(lit(i_ccworkday), 3)
                    ).otherwise(col("p_end_probation_date")),
                ).otherwise(col("end_probation_date")),
            )
        )

        # Remove joined columns and aliases
        # df_t17 = df_t17.drop("t01_nrp_customer", "t01_max_dpd", "t01_customer_segment")
        # df_t17 = df_t17.select(*[col(c).alias(c) for c in df_t17.columns])

        # Second MERGE operation: Update for RETAIL customers (deal level)
        df_t17 = (
            df_t17.withColumn(
                "default_indicator",
                when(
                    (col("retail_nonretail_indicator") == "RETAIL")
                    & (
                        col("p_default_reason").rlike(
                            "DPD|DEBT SALE|FORECLOSURE COLLATERAL|POCI|CROSS|CREDIT RISK ADJUSTMENT|CONTRACT TERMINATION|NON ACCRUED STATUS|BREACH OF COVENANTS"
                        )
                    )
                    & (col("p_is_probation") == 0)
                    & (col("proposed_default_ind") == "0")
                    & (col("p_proposed_default_ind") == "1"),
                    lit("1"),
                ).otherwise(col("default_indicator")),
            )
            .withColumn(
                "proposed_default_ind",
                when(
                    (col("retail_nonretail_indicator") == "RETAIL")
                    & (
                        col("p_default_reason").rlike(
                            "DPD|DEBT SALE|FORECLOSURE COLLATERAL|POCI|CROSS|CREDIT RISK ADJUSTMENT|CONTRACT TERMINATION|NON ACCRUED STATUS|BREACH OF COVENANTS"
                        )
                    )
                    & (col("p_is_probation") == 0)
                    & (col("proposed_default_ind") == "0")
                    & (col("p_proposed_default_ind") == "1"),
                    when(col("dpd_ho") > 0, "1").otherwise("0"),
                ).otherwise(col("proposed_default_ind")),
            )
            .withColumn(
                "default_reason",
                when(
                    (col("retail_nonretail_indicator") == "RETAIL")
                    & (
                        col("p_default_reason").rlike(
                            "DPD|DEBT SALE|FORECLOSURE COLLATERAL|POCI|CROSS|CREDIT RISK ADJUSTMENT|CONTRACT TERMINATION|NON ACCRUED STATUS|BREACH OF COVENANTS"
                        )
                    )
                    & (col("p_is_probation") == 0)
                    & (col("proposed_default_ind") == "0")
                    & (col("p_proposed_default_ind") == "1"),
                    when(col("dpd_ho") > 0, col("p_default_reason")).otherwise(
                        F.concat(lit("PRB "), col("p_default_reason"))
                    ),
                ).otherwise(col("default_reason")),
            )
            .withColumn(
                "is_probation",
                when(
                    (col("retail_nonretail_indicator") == "RETAIL")
                    & (
                        col("p_default_reason").rlike(
                            "DPD|DEBT SALE|FORECLOSURE COLLATERAL|POCI|CROSS|CREDIT RISK ADJUSTMENT|CONTRACT TERMINATION|NON ACCRUED STATUS|BREACH OF COVENANTS"
                        )
                    )
                    & (col("p_is_probation") == 0)
                    & (col("proposed_default_ind") == "0")
                    & (col("p_proposed_default_ind") == "1"),
                    when(col("dpd_ho") > 0, "0").otherwise("1"),
                ).otherwise(col("is_probation")),
            )
            .withColumn(
                "start_probation_date",
                when(
                    (col("retail_nonretail_indicator") == "RETAIL")
                    & (
                        col("p_default_reason").rlike(
                            "DPD|DEBT SALE|FORECLOSURE COLLATERAL|POCI|CROSS|CREDIT RISK ADJUSTMENT|CONTRACT TERMINATION|NON ACCRUED STATUS|BREACH OF COVENANTS"
                        )
                    )
                    & (col("p_is_probation") == 0)
                    & (col("proposed_default_ind") == "0")
                    & (col("p_proposed_default_ind") == "1"),
                    when(col("dpd_ho") > 0, lit(i_ccworkday)).otherwise(
                        col("p_start_probation_date")
                    ),
                ).otherwise(col("start_probation_date")),
            )
            .withColumn(
                "end_probation_date",
                when(
                    (col("retail_nonretail_indicator") == "RETAIL")
                    & (
                        col("p_default_reason").rlike(
                            "DPD|DEBT SALE|FORECLOSURE COLLATERAL|POCI|CROSS|CREDIT RISK ADJUSTMENT|CONTRACT TERMINATION|NON ACCRUED STATUS|BREACH OF COVENANTS"
                        )
                    )
                    & (col("p_is_probation") == 0)
                    & (col("proposed_default_ind") == "0")
                    & (col("p_proposed_default_ind") == "1"),
                    when(col("dpd_ho") > 0, add_months(lit(i_ccworkday), 3)).otherwise(
                        col("p_end_probation_date")
                    ),
                ).otherwise(col("end_probation_date")),
            )
        )

        # Third MERGE operation: Update for specific conditions
        df_t17 = (
            df_t17.withColumn(
                "default_indicator",
                when(
                    (
                        col("p_default_reason").like("%BANKRUPTACY%")
                        | col("p_default_reason").like("%DECEASED%")
                        | col("p_default_reason").like("%LOSS OF SUFFICIENT INCOME%")
                        | col("p_default_reason").like("%INDEBTEDNESS%")
                    )
                    & (
                        col("working_day").between(
                            col("p_start_probation_date"), col("p_end_probation_date")
                        )
                    )
                    & (col("is_probation") == 1),
                    lit("1"),
                ).otherwise(col("default_indicator")),
            )
            .withColumn(
                "proposed_default_ind",
                when(
                    (
                        col("p_default_reason").like("%BANKRUPTACY%")
                        | col("p_default_reason").like("%DECEASED%")
                        | col("p_default_reason").like("%LOSS OF SUFFICIENT INCOME%")
                        | col("p_default_reason").like("%INDEBTEDNESS%")
                    )
                    & (
                        col("working_day").between(
                            col("p_start_probation_date"), col("p_end_probation_date")
                        )
                    )
                    & (col("is_probation") == 1),
                    lit("0"),
                ).otherwise(col("proposed_default_ind")),
            )
            .withColumn(
                "default_reason",
                when(
                    (
                        col("p_default_reason").like("%BANKRUPTACY%")
                        | col("p_default_reason").like("%DECEASED%")
                        | col("p_default_reason").like("%LOSS OF SUFFICIENT INCOME%")
                        | col("p_default_reason").like("%INDEBTEDNESS%")
                    )
                    & (
                        col("working_day").between(
                            col("p_start_probation_date"), col("p_end_probation_date")
                        )
                    )
                    & (col("is_probation") == 1),
                    when(col("p_is_probation") == 0, col("default_reason")).otherwise(
                        col("p_default_reason")
                    ),
                ).otherwise(col("default_reason")),
            )
            .withColumn(
                "is_probation",
                when(
                    (
                        col("p_default_reason").like("%BANKRUPTACY%")
                        | col("p_default_reason").like("%DECEASED%")
                        | col("p_default_reason").like("%LOSS OF SUFFICIENT INCOME%")
                        | col("p_default_reason").like("%INDEBTEDNESS%")
                    )
                    & (
                        col("working_day").between(
                            col("p_start_probation_date"), col("p_end_probation_date")
                        )
                    )
                    & (col("is_probation") == 1),
                    lit("1"),
                ).otherwise(col("is_probation")),
            )
            .withColumn(
                "start_probation_date",
                when(
                    (
                        col("p_default_reason").like("%BANKRUPTACY%")
                        | col("p_default_reason").like("%DECEASED%")
                        | col("p_default_reason").like("%LOSS OF SUFFICIENT INCOME%")
                        | col("p_default_reason").like("%INDEBTEDNESS%")
                    )
                    & (
                        col("working_day").between(
                            col("p_start_probation_date"), col("p_end_probation_date")
                        )
                    )
                    & (col("is_probation") == 1),
                    when(col("t01_max_dpd") > 10, lit(i_ccworkday)).otherwise(
                        col("p_start_probation_date")
                    ),
                ).otherwise(col("start_probation_date")),
            )
            .withColumn(
                "end_probation_date",
                when(
                    (
                        col("p_default_reason").like("%BANKRUPTACY%")
                        | col("p_default_reason").like("%DECEASED%")
                        | col("p_default_reason").like("%LOSS OF SUFFICIENT INCOME%")
                        | col("p_default_reason").like("%INDEBTEDNESS%")
                    )
                    & (
                        col("working_day").between(
                            col("p_start_probation_date"), col("p_end_probation_date")
                        )
                    )
                    & (col("is_probation") == 1),
                    when(
                        col("t01_max_dpd") > 10, add_months(lit(i_ccworkday), 3)
                    ).otherwise(col("p_end_probation_date")),
                ).otherwise(col("end_probation_date")),
            )
        )

        # Remove joined columns and aliases
        df_t17 = df_t17.drop("t01_nrp_customer", "t01_max_dpd", "t01_customer_segment")
        df_t17 = df_t17.select(*[col(c).alias(c) for c in df_t17.columns])

        # Fourth UPDATE operation: Update for RETAIL customers with specific conditions
        df_t17 = (
            df_t17.withColumn(
                "default_indicator",
                when(
                    (col("retail_nonretail_indicator") == "RETAIL")
                    & (
                        col("p_default_reason").rlike(
                            "DPD|DEBT SALE|FORECLOSURE COLLATERAL|POCI|CROSS|CREDIT RISK ADJUSTMENT|CONTRACT TERMINATION|NON ACCRUED STATUS|BREACH OF COVENANTS"
                        )
                    )
                    & (
                        col("working_day").between(
                            col("p_start_probation_date"), col("p_end_probation_date")
                        )
                    )
                    & (col("is_probation") == 1),
                    lit("1"),
                ).otherwise(col("default_indicator")),
            )
            .withColumn(
                "proposed_default_ind",
                when(
                    (col("retail_nonretail_indicator") == "RETAIL")
                    & (
                        col("p_default_reason").rlike(
                            "DPD|DEBT SALE|FORECLOSURE COLLATERAL|POCI|CROSS|CREDIT RISK ADJUSTMENT|CONTRACT TERMINATION|NON ACCRUED STATUS|BREACH OF COVENANTS"
                        )
                    )
                    & (
                        col("working_day").between(
                            col("p_start_probation_date"), col("p_end_probation_date")
                        )
                    )
                    & (col("is_probation") == 1),
                    lit("0"),
                ).otherwise(col("proposed_default_ind")),
            )
            .withColumn(
                "default_reason",
                when(
                    (col("retail_nonretail_indicator") == "RETAIL")
                    & (
                        col("p_default_reason").rlike(
                            "DPD|DEBT SALE|FORECLOSURE COLLATERAL|POCI|CROSS|CREDIT RISK ADJUSTMENT|CONTRACT TERMINATION|NON ACCRUED STATUS|BREACH OF COVENANTS"
                        )
                    )
                    & (
                        col("working_day").between(
                            col("p_start_probation_date"), col("p_end_probation_date")
                        )
                    )
                    & (col("is_probation") == 1),
                    when(col("p_is_probation") == 0, col("default_reason")).otherwise(
                        col("p_default_reason")
                    ),
                ).otherwise(col("default_reason")),
            )
            .withColumn(
                "is_probation",
                when(
                    (col("retail_nonretail_indicator") == "RETAIL")
                    & (
                        col("p_default_reason").rlike(
                            "DPD|DEBT SALE|FORECLOSURE COLLATERAL|POCI|CROSS|CREDIT RISK ADJUSTMENT|CONTRACT TERMINATION|NON ACCRUED STATUS|BREACH OF COVENANTS"
                        )
                    )
                    & (
                        col("working_day").between(
                            col("p_start_probation_date"), col("p_end_probation_date")
                        )
                    )
                    & (col("is_probation") == 1),
                    lit("1"),
                ).otherwise(col("is_probation")),
            )
            .withColumn(
                "start_probation_date",
                when(
                    (col("retail_nonretail_indicator") == "RETAIL")
                    & (
                        col("p_default_reason").rlike(
                            "DPD|DEBT SALE|FORECLOSURE COLLATERAL|POCI|CROSS|CREDIT RISK ADJUSTMENT|CONTRACT TERMINATION|NON ACCRUED STATUS|BREACH OF COVENANTS"
                        )
                    )
                    & (
                        col("working_day").between(
                            col("p_start_probation_date"), col("p_end_probation_date")
                        )
                    )
                    & (col("is_probation") == 1),
                    when(col("dpd_ho") > 10, lit(i_ccworkday)).otherwise(
                        col("p_start_probation_date")
                    ),
                ).otherwise(col("start_probation_date")),
            )
            .withColumn(
                "end_probation_date",
                when(
                    (col("retail_nonretail_indicator") == "RETAIL")
                    & (
                        col("p_default_reason").rlike(
                            "DPD|DEBT SALE|FORECLOSURE COLLATERAL|POCI|CROSS|CREDIT RISK ADJUSTMENT|CONTRACT TERMINATION|NON ACCRUED STATUS|BREACH OF COVENANTS"
                        )
                    )
                    & (
                        col("working_day").between(
                            col("p_start_probation_date"), col("p_end_probation_date")
                        )
                    )
                    & (col("is_probation") == 1),
                    when(col("dpd_ho") > 10, add_months(lit(i_ccworkday), 3)).otherwise(
                        col("p_end_probation_date")
                    ),
                ).otherwise(col("end_probation_date")),
            )
        )

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

        print("calculate_probation_cc completed successfully.")
    except Exception as e:
        print(f"Error in calculate_probation_cc: {str(e)}")


if __name__ == "__main__":
    calc_probation_cc()
