# Third-party imports
from pyspark.sql.functions import col, when, lit, coalesce, expr
import pyspark.sql.functions as F
from src.insert_atmp_t17.calc_gca_value_refactored import GcsEtl

from src.unwinding.do_calculations_unwinding_cc import do_calculations_unwinding_cc

from src.insert_default_event.insert_default_events_cc import insert_default_events_cc

from src.insert_default.calculate_cc_monthly_recovery import (
    calculate_cc_monthly_recovery,
)

from src.insert_default.calculate_cc_default import calculate_cc_default

# Local imports
from db_connection_util import (
    get_spark_session,
    get_postgres_properties,
    read_data_from_postgres,
    update_to_database,
)

# Relative imports from current package
from .calculate_cc_dpd_ho2 import calculate_cc_dpd_ho2
from .calculate_cc_dpd_ho import calculate_cc_dpd_ho
from .calc_gross_interest_mtd import calc_gross_interest_mtd


def calculate_atmp_cc_formulas():
    spark = get_spark_session()
    postgres_properties = get_postgres_properties()
    schema_name = "ABACUS_A5"

    # Read necessary tables
    df_t17 = read_data_from_postgres(spark, "atmp_t17_dpd_credit_cards", schema_name)
    df_t01 = read_data_from_postgres(spark, "t01_customer", schema_name)
    df_customer = read_data_from_postgres(spark, "atmp_customer", schema_name)

    # Perform all operations on the DataFrame
    df_t17 = (
        df_t17.alias("t17")
        .join(
            df_t01.select(
                col("nrp_customer").alias("t01_nrp_customer"),
                col("retail_nonretail").alias("t01_retail_nonretail"),
            ).alias("t01"),
            col("t17.customer_number") == col("t01_nrp_customer"),
            "left_outer",
        )
        .withColumn(
            "retail_nonretail_indicator",
            coalesce(col("t01_retail_nonretail"), col("retail_nonretail_indicator")),
        )
        .drop("t01_nrp_customer", "t01_retail_nonretail")
    )

    # Write to the final dataframe only the needed columns
    df_updated = df_t17.select("id_product", "retail_nonretail_indicator")

    # Write the final dataframe to a temporary table
    df_updated.write.jdbc(
        url=postgres_properties["url"],
        table='"ABACUS_A5"."temp_updated_rows"',
        mode="overwrite",  # Use overwrite for temporary table
        properties=postgres_properties,
    )

    update_query = """
        UPDATE "ABACUS_A5"."atmp_t17_dpd_credit_cards" AS t17
        SET retail_nonretail_indicator = temp.retail_nonretail_indicator
        FROM "ABACUS_A5"."temp_updated_rows" AS temp
        WHERE t17.id_product = temp.id_product;
        """

    update_to_database(update_query)

    # Call the previously defined functions
    print("Calling calculate_cc_dpd_ho()")
    calculate_cc_dpd_ho()
    print("Calling calculate_cc_dpd_ho2()")
    calculate_cc_dpd_ho2()

    # Read the updated data from the table
    df_t17 = read_data_from_postgres(spark, "atmp_t17_dpd_credit_cards", schema_name)

    # Update times_past_due
    df_t17 = df_t17.withColumn(
        "times_past_due",
        when(
            (col("p_dpd_ho") == 0) & (col("dpd_ho") > 0),
            coalesce(col("p_times_past_due"), lit(0)) + 1,
        ).otherwise(coalesce(col("p_times_past_due"), lit(0))),
    )

    # Update maximum_days_past_due
    df_t17 = df_t17.withColumn(
        "maximum_days_past_due",
        when(
            col("dpd_ho") > coalesce(col("p_maximum_days_past_due"), lit(0)),
            col("dpd_ho"),
        ).otherwise(coalesce(col("p_maximum_days_past_due"), lit(0))),
    )

    # MERGE operation: Update t17 based on customer table (df_customer)
    df_t17 = (
        df_t17.alias("t17")
        .join(
            df_customer.select(
                col("nrp_customer").alias("customer_nrp_customer"),
                col("maximum_days_past_due").alias("customer_maximum_days_past_due"),
            ).alias("customer"),
            col("t17.customer_number") == col("customer_nrp_customer"),
            "left_outer",
        )
        .withColumn(
            "maximum_days_past_due",
            when(
                (col("t17.retail_nonretail_indicator") == "NONRETAIL")
                & (col("customer_nrp_customer").isNotNull()),
                col("customer_maximum_days_past_due"),
            ).otherwise(col("maximum_days_past_due")),
        )
        .drop("customer_nrp_customer", "customer_maximum_days_past_due")
    )

    # Removing aliases to avoid ambiguous references in the later joins
    df_t17 = df_t17.select(*[col(c).alias(c) for c in df_t17.columns])

    # Update total_days_past_due
    df_t17 = df_t17.withColumn(
        "total_days_past_due",
        when(
            col("dpd_ho") > 0, coalesce(col("p_total_days_past_due"), lit(0)) + 1
        ).otherwise(coalesce(col("p_total_days_past_due"), lit(0))),
    )

    # Update current_days_in_excess
    df_t17 = df_t17.withColumn(
        "current_days_in_excess",
        when((col("dpd_ho") > 0) & (expr("EXTRACT(DAY FROM working_day) = 1")), 1)
        .when(
            (col("dpd_ho") > 0) & (expr("EXTRACT(DAY FROM working_day) <> 1")),
            coalesce(col("p_current_days_in_excess"), lit(0)) + 1,
        )
        .otherwise(lit(0)),
    )

    # Update maxdpd_current_month using func_maxdpd_month
    # df_t17 = df_t17.withColumn(
    #    "maxdpd_current_month",
    #    func_maxdpd_month(col("maxdpd_month"), col("dpd_ho"), col("working_day"))
    # )

    # Bypassing the func_maxdpd_month function. TO BE REVIEWED LATER!
    df_t17 = df_t17.withColumn(
        "maxdpd_current_month",
        when(
            col("retail_nonretail_indicator") == "RETAIL",
            when(
                F.date_format(col("working_day"), "dd") == "01", col("dpd_ho")
            ).otherwise(
                when(
                    coalesce(col("maxdpd_month"), lit(0)) > col("dpd_ho"),
                    coalesce(col("maxdpd_month"), lit(0)),
                ).otherwise(col("dpd_ho"))
            ),
        ).otherwise(col("maxdpd_current_month")),
    )

    # Update maxdpd_current_month for NONRETAIL
    df_t17 = (
        df_t17.alias("t17")
        .join(
            df_customer.select(
                col("nrp_customer").alias("customer_nrp_customer"),
                col("maxdpd_current_month").alias("customer_maxdpd_current_month"),
            ).alias("customer"),
            col("t17.customer_number") == col("customer_nrp_customer"),
            "left_outer",
        )
        .withColumn(
            "maxdpd_current_month",
            when(
                (col("t17.retail_nonretail_indicator") == "NONRETAIL")
                & (col("customer_nrp_customer").isNotNull()),
                col("customer_maxdpd_current_month"),
            ).otherwise(col("maxdpd_current_month")),
        )
        .drop("customer_nrp_customer", "customer_maxdpd_current_month")
    )

    # Removing aliases to avoid ambiguous references in the later joins
    df_t17 = df_t17.select(*[col(c).alias(c) for c in df_t17.columns])

    # Update absorbing_status_flag
    df_t17 = df_t17.withColumn(
        "absorbing_status_flag",
        when(col("maximum_days_past_due") >= 180, 1).otherwise(0),
    )

    # Update date_absorbing_status
    df_t17 = df_t17.withColumn(
        "date_absorbing_status",
        when(
            (col("absorbing_status_flag") == 1)
            & (col("p_date_absorbing_status").isNull()),
            col("working_day"),
        ).otherwise(col("p_date_absorbing_status")),
    )

    # Update exposure_at_absorbing
    df_t17 = df_t17.withColumn(
        "exposure_at_absorbing",
        when(
            (col("absorbing_status_flag") == 1)
            & (coalesce(col("p_exposure_at_absorbing"), lit(0)) == 0)
            & (col("card_balance") > 0),
            col("card_balance"),
        ).otherwise(coalesce(col("p_exposure_at_absorbing"), lit(0))),
    )

    # Write to the final dataframe only the needed columns
    df_updated = df_t17.select(
        "id_product",
        "times_past_due",
        "maximum_days_past_due",
        "total_days_past_due",
        "current_days_in_excess",
        "maxdpd_current_month",
        "absorbing_status_flag",
        "date_absorbing_status",
        "exposure_at_absorbing",
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
                times_past_due = temp.times_past_due,
                maximum_days_past_due = temp.maximum_days_past_due,
                total_days_past_due = temp.total_days_past_due,
                current_days_in_excess = temp.current_days_in_excess,
                maxdpd_current_month = temp.maxdpd_current_month,
                absorbing_status_flag = temp.absorbing_status_flag,
                date_absorbing_status = temp.date_absorbing_status,
                exposure_at_absorbing = temp.exposure_at_absorbing
            FROM "ABACUS_A5"."temp_updated_rows" AS temp
            WHERE t17.id_product = temp.id_product;
            """

    update_to_database(update_query)

    # Call external procedures
    print("Calling calculate_cc_default...")
    calculate_cc_default()
    print("Calling calculate_cc_monthly_recovery...")
    calculate_cc_monthly_recovery()
    print("Calling insert_default_events_cc...")
    insert_default_events_cc()

    # Read the updated data after external procedures
    df_t17 = read_data_from_postgres(spark, "atmp_t17_dpd_credit_cards", schema_name)

    # Update eba_status and eba_status_start_date
    df_t17 = df_t17.withColumn(
        "eba_status", when(col("default_indicator") == "1", "NPE").otherwise("PE")
    ).withColumn(
        "eba_status_start_date",
        when(
            (col("default_indicator") == "1") & (col("p_default_indicator") == "0"),
            col("working_day"),
        )
        .when(
            (col("default_indicator") == "0") & (col("p_default_indicator") == "1"),
            col("working_day"),
        )
        .otherwise(coalesce(col("eba_status_start_date"), col("working_day"))),
    )

    df_updated = df_t17.select("id_product", "eba_status", "eba_status_start_date")

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
                        eba_status = temp.eba_status,
                        eba_status_start_date = temp.eba_status_start_date
                    FROM "ABACUS_A5"."temp_updated_rows" AS temp
                    WHERE t17.id_product = temp.id_product;
                    """

    update_to_database(update_query)

    # Call additional calculations
    print("Calling calc_gca_value...")
    gca_etl = GcsEtl()
    gca_etl.run()
    print("Calling do_calculations_unwinding_cc...")
    do_calculations_unwinding_cc()
    print("Calling calc_grossinterest_mtd...")
    calc_gross_interest_mtd()

    print("calculate_atmp_cc_formulas completed succesfully.")


if __name__ == "__main__":
    calculate_atmp_cc_formulas()
