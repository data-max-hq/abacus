from src.functions.func_calc_gross_interest_mtd import calc_gross_interest_mtd_sql
from db_connection_util import (
    get_spark_session,
    get_postgres_properties,
    read_data_from_postgres,
    update_to_database,
)


def calc_gross_interest_mtd():
    try:
        # Get Spark session and Postgres properties
        spark = get_spark_session()
        postgres_properties = get_postgres_properties()
        schema_name = "ABACUS_A5"

        # Load the necessary table
        df_t17 = read_data_from_postgres(
            spark, "atmp_t17_dpd_credit_cards", schema_name
        )

        # Apply the SQL-based calculation for gross interest MTD
        df_updated = calc_gross_interest_mtd_sql(df_t17)

        # Write the updated DataFrame to a temporary table in the database
        df_updated.write.jdbc(
            url=postgres_properties["url"],
            table='"ABACUS_A5"."temp_updated_rows"',
            mode="overwrite",
            properties=postgres_properties,
        )

        # SQL query to update the actual table
        update_query = """
                        UPDATE "ABACUS_A5"."atmp_t17_dpd_credit_cards" AS t17
                        SET 
                            gross_interest_mtd_lcy = temp.gross_interest_mtd_lcy,
                            gross_interest_mtd_ocy = temp.gross_interest_mtd_ocy
                        FROM "ABACUS_A5"."temp_updated_rows" AS temp
                        WHERE t17.id_product = temp.id_product;
                        """

        # Execute the update query
        update_to_database(update_query)

        print("calc_grossinterest_mtd completed successfully.")

    except Exception as e:
        print(f"Error in calc_grossinterest_mtd: {str(e)}")


if __name__ == "__main__":
    calc_gross_interest_mtd()
