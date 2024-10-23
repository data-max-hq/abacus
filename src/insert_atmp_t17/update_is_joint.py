from pyspark.sql.functions import col, lit, when
from db_connection_util import get_spark_session, get_postgres_properties


def update_is_joint():
    spark = get_spark_session()
    postgres_properties = get_postgres_properties()

    # Read data from relevant tables
    df_customer_relations = spark.read.jdbc(
        table="customer_relations", properties=postgres_properties
    )
    df_card_balance = spark.read.jdbc(
        table="t22_card_balance", properties=postgres_properties
    )
    df_working_day = spark.read.jdbc(
        table="w01_working_day", properties=postgres_properties
    )

    # Get the latest working day
    latest_working_day = df_working_day.agg({"working_day": "max"}).collect()[0][0]

    # Filter card balance data
    df_filtered_card_balance = df_card_balance.filter(
        col("working_day") == latest_working_day
    )

    # Join tables
    df_joined = df_customer_relations.alias("t").join(
        df_filtered_card_balance.alias("t22"),
        (col("t22.customer_number") == col("t.applicant_id"))
        & (col("t22.facility_id").substr(7, 3) == col("t.facility_id").substr(3, 3))
        & (col("t22.facility_id").substr(10, 2) == col("t.facility_id").substr(1, 2)),
        "inner",
    )

    # Perform update logic
    df_updated = df_joined.withColumn(
        "is_joint",
        when(
            (col("is_joint_micro") == "1")
            & (col("application_role_code") == "BORROWER")
            & (col("product_type") != "LOAN"),
            lit("1"),
        ).otherwise(col("is_joint")),
    ).withColumn(
        "joint_id",
        when(
            (col("is_joint_micro") == "1")
            & (col("application_role_code") == "BORROWER")
            & (col("product_type") != "LOAN"),
            col("joint_group"),
        ).otherwise(col("joint_id")),
    )

    # Write back to the database
    df_updated.write.jdbc(
        table="atmp_t17_dpd_credit_cards",
        properties=postgres_properties,
        mode="overwrite",  # Use 'overwrite' to update existing records
    )

    spark.stop()


if __name__ == "__main__":
    update_is_joint()
