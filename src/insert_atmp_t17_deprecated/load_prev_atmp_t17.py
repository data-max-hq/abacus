from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit
from db_connection_util import (
    get_spark_session,
    get_postgres_properties,
    read_data_from_postgres,
    update_to_database,
)


def load_prev_atmp_t17():
    try:
        spark = get_spark_session()
        postgres_properties = get_postgres_properties()
        schema_name = "ABACUS_A5"

        # Load necessary tables
        df_atmp_t17 = read_data_from_postgres(
            spark, "atmp_t17_dpd_credit_cards", schema_name
        )
        df_t17 = read_data_from_postgres(spark, "t17_dpd_credit_cards", schema_name)
        df_w02 = read_data_from_postgres(spark, "w02_ccworking_day", schema_name)

        # Get the previous working day
        max_working_day = df_w02.agg(F.max("working_day")).collect()[0][0]
        prev_working_day = (
            df_w02.filter(col("working_day") < lit(max_working_day))
            .agg(F.max("working_day"))
            .collect()[0][0]
        )

        # Filter t17_dpd_credit_cards for the previous working day
        df_t17_prev = df_t17.filter(col("working_day") == lit(prev_working_day))

        # Prepare the update dataframe
        df_updates = df_t17_prev.select(
            col("id_product"),
            col("amount_past_due").alias("p_amount_past_due"),
            col("dpd_ho").alias("p_dpd_ho"),
            col("times_past_due").alias("p_times_past_due"),
            col("maximum_days_past_due").alias("p_maximum_days_past_due"),
            col("total_days_past_due").alias("p_total_days_past_due"),
            col("current_days_in_excess").alias("p_current_days_in_excess"),
            col("date_absorbing_status").alias("p_date_absorbing_status"),
            col("exposure_at_absorbing").alias("p_exposure_at_absorbing"),
            col("historical_default_indicator").alias("p_historical_default_indicator"),
            col("default_indicator").alias("p_default_indicator"),
            col("default_amount").alias("p_default_amount"),
            col("default_start_date").alias("p_default_start_date"),
            col("default_end_date").alias("p_default_end_date"),
            col("monthly_recover_default").alias("p_monthly_recover_default"),
            col("monthly_recover_absorbing").alias("p_monthly_recover_absorbing"),
            col("proposed_default_ind").alias("p_proposed_default_ind"),
            col("is_probation").alias("p_is_probation"),
            col("start_probation_date").alias("p_start_probation_date"),
            col("end_probation_date").alias("p_end_probation_date"),
            col("default_reason").alias("p_default_reason"),
            col("default_id"),
            col("interest_rate_default"),
            col("date_since_pd_ho").alias("p_date_since_pd_ho"),
            col("date_since_pd_ho2").alias("p_date_since_pd_ho2"),
            col("eba_status_start_date"),
            col("maxdpd_current_month").alias("maxdpd_month"),
            col("gross_interest_mtd_lcy"),
            col("gross_interest_mtd_ocy"),
        )
        df_atmp_t17.show()
        df_updates.show()
        # Write updates to a temporary table
        df_updates.write.jdbc(
            url=postgres_properties["url"],
            table=f'"{schema_name}"."temp_atmp_t17_updates"',
            mode="overwrite",
            properties=postgres_properties,
        )

        # Update existing records
        update_query = f"""
            UPDATE "{schema_name}"."atmp_t17_dpd_credit_cards" AS d
            SET
                p_amount_past_due = s.p_amount_past_due,
                p_dpd_ho = s.p_dpd_ho,
                p_times_past_due = s.p_times_past_due,
                p_maximum_days_past_due = s.p_maximum_days_past_due,
                p_total_days_past_due = s.p_total_days_past_due,
                p_current_days_in_excess = s.p_current_days_in_excess,
                p_date_absorbing_status = s.p_date_absorbing_status,
                p_exposure_at_absorbing = s.p_exposure_at_absorbing,
                p_historical_default_indicator = s.p_historical_default_indicator,
                p_default_indicator = s.p_default_indicator,
                p_default_amount = s.p_default_amount,
                p_default_start_date = s.p_default_start_date,
                p_default_end_date = s.p_default_end_date,
                p_monthly_recover_default = s.p_monthly_recover_default,
                p_monthly_recover_absorbing = s.p_monthly_recover_absorbing,
                p_proposed_default_ind = s.p_proposed_default_ind,
                p_is_probation = s.p_is_probation,
                p_start_probation_date = s.p_start_probation_date,
                p_end_probation_date = s.p_end_probation_date,
                p_default_reason = s.p_default_reason,
                default_id = s.default_id,
                interest_rate_default = s.interest_rate_default,
                p_date_since_pd_ho = s.p_date_since_pd_ho,
                p_date_since_pd_ho2 = s.p_date_since_pd_ho2,
                eba_status_start_date = s.eba_status_start_date,
                maxdpd_month = s.maxdpd_month,
                gross_interest_mtd_lcy = s.gross_interest_mtd_lcy,
                gross_interest_mtd_ocy = s.gross_interest_mtd_ocy
            FROM "{schema_name}"."temp_atmp_t17_updates" AS s
            WHERE d.id_product = s.id_product;
        """
        update_to_database(update_query)

        # Update p_working_day for all records
        update_working_day_query = f"""
            UPDATE "{schema_name}"."atmp_t17_dpd_credit_cards"
            SET p_working_day = '{prev_working_day}';
        """
        update_to_database(update_working_day_query)

        print("load_prev_atmp_t17 completed successfully.")
        return True
    except Exception as e:
        return False
        print(f"Error in load_prev_atmp_t17: {str(e)}")


if __name__ == "__main__":
    load_prev_atmp_t17()
