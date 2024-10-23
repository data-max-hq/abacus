from pyspark.sql import functions as F
from pyspark.sql.functions import col
from db_connection_util import (
    get_spark_session,
    get_postgres_properties,
    read_data_from_postgres,
)


def commit_atmpt18_into_t18():
    try:
        spark = get_spark_session()
        postgres_properties = get_postgres_properties()
        schema_name = "ABACUS_A5"

        # Load necessary table
        df_atmp_t18 = read_data_from_postgres(
            spark, "atmp_t18_cc_payment_schedule", schema_name
        )

        # Get the current maximum autoid from t17_dpd_credit_cards
        df_max_autoid = read_data_from_postgres(
            spark, "t18_cc_payment_schedule", schema_name
        ).agg(F.max("autoid").alias("max_autoid"))
        max_autoid = df_max_autoid.collect()[0]["max_autoid"] or 0

        # Prepare the dataframe for insertion
        df_insert = df_atmp_t18.select(
            (max_autoid + F.monotonically_increasing_id() + 1).alias("autoid"),
            col("id_product"),
            col("principal_payment_date"),
            col("principal_payment_amount"),
            col("interest_payment_date"),
            col("interest_payment_amount"),
            col("customer_number"),
            col("minimum_payment"),
            col("penalty_interest_rate"),
            col("penalty_interest_amount"),
            col("working_day"),
            col("last_sum_of_payment"),
            col("payment_amount"),
            col("is_pastdue"),
            col("period"),
            col("due_date_dlq"),
        )

        # Insert the data into t18_cc_payment_schedule
        df_insert.write.jdbc(
            url=postgres_properties["url"],
            table=f'"{schema_name}"."t18_cc_payment_schedule"',
            mode="append",
            properties=postgres_properties,
        )

        print("commit_atmpt18_into_t18 completed successfully.")
    except Exception as e:
        print(f"Error in commit_atmpt18_into_t18: {str(e)}")


if __name__ == "__main__":
    commit_atmpt18_into_t18()
