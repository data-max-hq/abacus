from pyspark.sql.functions import col
from db_connection_util import (
    get_spark_session,
    get_postgres_properties,
    read_data_from_postgres,
)


def check_customer():
    try:
        spark = get_spark_session()
        postgres_properties = get_postgres_properties()
        schema_name = "ABACUS_A5"

        # Load necessary tables
        df_t01 = read_data_from_postgres(spark, "t01_customer", schema_name)
        df_t17 = read_data_from_postgres(
            spark, "atmp_t17_dpd_credit_cards", schema_name
        )

        # Select only the customer_number from df_t17
        df_t17_customer = df_t17.select(
            col("customer_number").alias("t17_customer_number")
        )

        # Perform the merge operation
        df_merged = df_t01.join(
            df_t17_customer,
            col("nrp_customer") == col("t17_customer_number"),
            "full_outer",
        )

        # Identify new customers
        df_new_customers = df_merged.filter(col("nrp_customer").isNull()).select(
            col("t17_customer_number").alias("nrp_customer")
        )

        # Insert new customers into t01_customer
        if df_new_customers.count() > 0:
            df_new_customers.write.jdbc(
                url=postgres_properties["url"],
                table=f'"{schema_name}"."t01_customer"',
                mode="append",
                properties=postgres_properties,
            )

        print("check_customer completed successfully.")
        return True
    except Exception as e:
        return False
        print(f"Error in check_customer: {str(e)}")


if __name__ == "__main__":
    check_customer()
