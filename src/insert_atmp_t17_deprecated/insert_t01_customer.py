from pyspark.sql.functions import col, when, lit, coalesce
from db_connection_util import (
    get_spark_session,
    get_postgres_properties,
    read_data_from_postgres,
)
from is_existing_customer import is_existing_customer


def insert_t01_customer(
    inrp_customer,
    icustomer_name,
    icustomer_phone,
    icustomer_address,
    iprofit_center,
    icoconut_type,
    imarketing_group,
    icoconut_number,
    iofficer_code,
    iofficer_name,
    inace_code,
    iesa_code,
):
    try:
        spark = get_spark_session()
        postgres_properties = get_postgres_properties()
        schema_name = "ABACUS_A5"

        # Check if customer exists
        customer_exists = is_existing_customer(inrp_customer)

        if customer_exists == 0:
            # Prepare data for insertion
            data = [
                (
                    inrp_customer,
                    icustomer_name,
                    icustomer_phone,
                    icustomer_address,
                    iprofit_center,
                    icoconut_type,
                    imarketing_group,
                    icoconut_number,
                    iofficer_code,
                    iofficer_name,
                    inace_code,
                    iesa_code,
                )
            ]
            columns = [
                "nrp_customer",
                "customer_name",
                "customer_phone",
                "customer_address",
                "profit_center",
                "coconut_type",
                "marketing_group",
                "coconut_number",
                "customer_officer_code",
                "customer_officer_name",
                "nace_code",
                "esa_code",
            ]

            df_insert = spark.createDataFrame(data, columns)

            # Apply transformations
            df_insert = (
                df_insert.withColumn(
                    "retail_nonretail",
                    when(col("coconut_type").isin("01", "20"), "RETAIL").otherwise(
                        "NONRETAIL"
                    ),
                )
                .withColumn(
                    "customer_segment",
                    when(col("coconut_type") == "01", "PI")
                    .when(col("coconut_type") == "20", "MICRO")
                    .when(col("coconut_type") == "06", "SME")
                    .otherwise("CORPORATE"),
                )
                .withColumn("customer_name", coalesce(col("customer_name"), lit("")))
                .withColumn("customer_phone", coalesce(col("customer_phone"), lit("")))
                .withColumn(
                    "customer_address", coalesce(col("customer_address"), lit(""))
                )
                .withColumn("profit_center", coalesce(col("profit_center"), lit("")))
                .withColumn("coconut_type", coalesce(col("coconut_type"), lit("")))
                .withColumn(
                    "marketing_group", coalesce(col("marketing_group"), lit(""))
                )
                .withColumn("coconut_number", coalesce(col("coconut_number"), lit("")))
                .withColumn("nace_code", coalesce(col("nace_code"), lit("")))
                .withColumn("esa_code", coalesce(col("esa_code"), lit("")))
            )

            # Insert data
            df_insert.write.jdbc(
                url=postgres_properties["url"],
                table='"ABACUS_A5"."t01_customer"',
                mode="append",
                properties=postgres_properties,
            )

        # Update ESA_CODE
        df_t01 = read_data_from_postgres(spark, "t01_customer", schema_name)
        df_tc03 = read_data_from_postgres(spark, "TC03_PROFIT_CENTER", schema_name)

        df_update = (
            df_t01.join(df_tc03, df_t01.PROFIT_CENTER == df_tc03.PROFIT_CENTER, "left")
            .withColumn(
                "ESA_CODE",
                when(
                    (col("nrp_customer") == inrp_customer)
                    & (col("ESA_CODE").isNull() | (col("ESA_CODE") == "")),
                    col("tc03.ESA_CODE"),
                ).otherwise(col("t01.ESA_CODE")),
            )
            .drop(df_tc03.PROFIT_CENTER, df_tc03.ESA_CODE)
        )

        # Write updated data back to the database
        df_update.write.jdbc(
            url=postgres_properties["url"],
            table='"ABACUS_A5"."t01_customer"',
            mode="overwrite",
            properties=postgres_properties,
        )

        print("insert_t01_customer completed successfully.")
    except Exception as e:
        print(f"Error in insert_t01_customer: {str(e)}")
        import traceback

        traceback.print_exc()


# Main execution
if __name__ == "__main__":
    # You can call the functions here or expose them as needed
    insert_t01_customer()
