from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from src.common.structured_etl import StructuredEtl
from ..db_connection_util import (
    get_spark_session,
    get_postgres_properties,
    read_data_from_postgres,
)


class CustomerEtl(StructuredEtl):
    def __init__(self):
        self.postgres_properties = get_postgres_properties()

    def read(self) -> tuple[DataFrame, DataFrame]:
        spark = get_spark_session()
        schema_name = "ABACUS_A5"

        # Load necessary tables
        df_t01 = read_data_from_postgres(spark, "t01_customer", schema_name)
        df_t17 = read_data_from_postgres(
            spark, "atmp_t17_dpd_credit_cards", schema_name
        )

        return df_t01, df_t17

    def transform(self, df_t01: DataFrame, df_t17: DataFrame) -> DataFrame:
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

        return df_new_customers

    def persist(self, df_new_customers: DataFrame) -> None:
        # Insert new customers into t01_customer if there are any
        if df_new_customers.count() > 0:
            df_new_customers.write.jdbc(
                url=self.postgres_properties["url"],
                table='"ABACUS_A5"."t01_customer"',
                mode="append",
                properties=self.postgres_properties,
            )

    def run(self) -> None:
        try:
            print("CustomerEtl processing started ...")
            df_t01, df_t17 = self.read()
            df_new_customers = self.transform(df_t01, df_t17)
            self.persist(df_new_customers)
            print("CustomerEtl processing completed successfully")
        except Exception as e:
            print(f"CustomerEtl processing failed with error: {str(e)}")


if __name__ == "__main__":
    customer_etl = CustomerEtl()
    customer_etl.run()
