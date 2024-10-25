from pyspark.sql.functions import col, trim, lit
from db_connection_util import (
    get_spark_session,
    get_postgres_properties,
    read_data_from_postgres,
)


def check_product_type():
    try:
        spark = get_spark_session()
        postgres_properties = get_postgres_properties()
        schema_name = "ABACUS_A5"

        # Load necessary tables
        df_tc01 = read_data_from_postgres(spark, "tc01_product_type", schema_name)
        df_t17 = read_data_from_postgres(
            spark, "atmp_t17_dpd_credit_cards", schema_name
        )

        # Prepare the source dataframe
        df_t17_distinct = df_t17.select(
            trim(col("id_product_type")).alias("t17_id_product_type"),
            lit("CREDIT CARD").alias("t17_product_group"),
        ).distinct()

        # Perform the merge operation
        df_merged = df_tc01.join(
            df_t17_distinct,
            trim(col("id_product_type")) == col("t17_id_product_type"),
            "full_outer",
        )

        # Identify new product types
        df_new_product_types = df_merged.filter(col("id_product_type").isNull()).select(
            col("t17_id_product_type").alias("id_product_type"),
            col("t17_product_group").alias("product_group"),
        )

        # Insert new product types into tc01_product_type
        if df_new_product_types.count() > 0:
            df_new_product_types.write.jdbc(
                url=postgres_properties["url"],
                table=f'"{schema_name}"."tc01_product_type"',
                mode="append",
                properties=postgres_properties,
            )

        print("check_product_type completed successfully.")
        return True
    except Exception as e:
        return False
        print(f"Error in check_product_type: {str(e)}")


if __name__ == "__main__":
    check_product_type()
