from pyspark.sql import functions as F
from pyspark.sql.functions import col
from db_connection_util import (
    get_spark_session,
    get_postgres_properties,
    read_data_from_postgres,
)


def commit_eatmpt17_into_t17():
    try:
        spark = get_spark_session()
        postgres_properties = get_postgres_properties()
        schema_name = "ABACUS_A5"

        # Load necessary table
        df_err_atmp_t17 = read_data_from_postgres(
            spark, "err_atmp_t17_dpd_credit_cards", schema_name
        )

        # Get the current maximum autoid from t17_dpd_credit_cards
        df_max_autoid = read_data_from_postgres(
            spark, "t17_dpd_credit_cards", schema_name
        ).agg(F.max("autoid").alias("max_autoid"))
        max_autoid = df_max_autoid.collect()[0]["max_autoid"] or 0

        # Prepare the dataframe for insertion
        df_insert = df_err_atmp_t17.select(
            (max_autoid + F.monotonically_increasing_id() + 1).alias("autoid"),
            col("working_day"),
            col("amount_past_due"),
            col("date_since_pd_ol"),
            col("days_past_due"),
            col("delinquency_amount_mp"),
            col("last_unpaid_due_date_mp"),
            col("minimum_payment"),
            col("ol_da"),
            col("ol_dpd"),
            col("branch_code"),
            col("customer_number"),
            col("account_code"),
            col("account_currency"),
            col("account_sequence"),
            col("account_number"),
            col("id_product"),
            col("id_product_type"),
            col("card_number"),
            col("card_balance"),
            col("card_expire_date"),
            col("card_ccy"),
            col("card_limit"),
            col("next_payment_date"),
            col("last_statement_balance"),
            col("sum_of_payments"),
            col("dpd_ho"),
            col("retail_nonretail_indicator"),
            col("times_past_due"),
            col("maximum_days_past_due"),
            col("total_days_past_due"),
            col("current_days_in_excess"),
            col("number_of_payments_past_due"),
            col("date_since_past_due"),
            col("absorbing_status_flag"),
            col("date_absorbing_status"),
            col("exposure_at_absorbing"),
            col("default_contagion_indicator"),
            col("default_indicator"),
            col("historical_default_indicator"),
            col("default_reason"),
            col("default_amount"),
            col("default_start_date"),
            col("default_end_date"),
            col("default_id"),
            col("monthly_recover"),
            col("monthly_recover_default"),
            col("monthly_recover_absorbing"),
        )

        # Insert the data into t17_dpd_credit_cards
        df_insert.write.jdbc(
            url=postgres_properties["url"],
            table=f'"{schema_name}"."t17_dpd_credit_cards"',
            mode="append",
            properties=postgres_properties,
        )

        print("commit_eatmpt17_into_t17 completed successfully.")
    except Exception as e:
        print(f"Error in commit_eatmpt17_into_t17: {str(e)}")


if __name__ == "__main__":
    commit_eatmpt17_into_t17()
