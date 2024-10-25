from src.common.structured_etl import StructuredEtl
from src.functions.func_calc_gross_interest_mtd import calc_gross_interest_mtd_sql
from ..db_connection_util import (
    get_spark_session,
    get_postgres_properties,
    read_data_from_postgres,
    update_to_database,
)

DPD_CREDIT_CARDS_TABLE = "atmp_t17_dpd_credit_cards"


class GrossInterestMtdEtl(StructuredEtl):
    def __init__(self):
        self.postgres_properties = get_postgres_properties()
        self.schema_name = "ABACUS_A5"

    def read(self):
        spark = get_spark_session()

        # Load the necessary table
        dpd_credit_cards_df = read_data_from_postgres(
            spark, DPD_CREDIT_CARDS_TABLE, self.schema_name
        )
        return (dpd_credit_cards_df,)

    def transform(self, dpd_credit_cards_df) -> tuple:
        # Apply the SQL-based calculation for gross interest MTD
        df_updated = calc_gross_interest_mtd_sql(dpd_credit_cards_df)
        return (df_updated,)

    def persist(self, df_updated) -> None:
        # Write the updated DataFrame to a temporary table
        df_updated.write.jdbc(
            url=self.postgres_properties["url"],
            table=f'"{self.schema_name}"."temp_updated_rows"',
            mode="overwrite",
            properties=self.postgres_properties,
        )

        # SQL query to update the actual table
        update_query = f"""
            UPDATE "{self.schema_name}"."{DPD_CREDIT_CARDS_TABLE}" AS t17
            SET 
                gross_interest_mtd_lcy = temp.gross_interest_mtd_lcy,
                gross_interest_mtd_ocy = temp.gross_interest_mtd_ocy
            FROM "{self.schema_name}"."temp_updated_rows" AS temp
            WHERE t17.id_product = temp.id_product;
        """

        # Execute the update query
        update_to_database(update_query)

    def run(self):
        try:
            print("GrossInterestMtdEtl processing started ...")
            (df_t17,) = self.read()
            result = self.transform(df_t17)
            self.persist(result[0])
            print("GrossInterestMtdEtl processing completed successfully")
        except Exception as e:
            print(f"GrossInterestMtdEtl processing failed with error: {str(e)}")


if __name__ == "__main__":
    gross_interest_mtd_etl = GrossInterestMtdEtl()
    gross_interest_mtd_etl.run()
