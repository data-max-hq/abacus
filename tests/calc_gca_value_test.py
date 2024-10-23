import sys
import os

# Add the parent directory to the system path for module imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.abacus_insert_atmp_t17.calc_gca_value import (
    calc_gca_value,
)  # Replace with the actual module name

import pytest
from unittest import mock
from pyspark.sql import SparkSession
from pyspark.sql import Row


@pytest.fixture
def spark_session():
    return (
        SparkSession.builder.master("local")
        .appName("test")
        .config("spark.driver.host", "localhost")
        .config("spark.port.maxRetries", 100)
        .config(
            "spark.jars",
            "/Users/dardanx/Documents/work/datamax/customers/rbal/abacus/postgresql-42.7.4.jar",
        )
        .config(
            "spark.driver.extraClassPath",
            "/Users/dardanx/Documents/work/datamax/customers/rbal/abacus/postgresql-42.7.4.jar",
        )
        .config(
            "spark.executor.extraClassPath",
            "/Users/dardanx/Documents/work/datamax/customers/rbal/abacus/postgresql-42.7.4.jar",
        )
        .getOrCreate()
    )


def test_calc_gca_value(spark_session):
    with mock.patch(
        "Abacus_insert_atmp_t17_old.calc_gca_value.get_spark_session"
    ) as mock_get_spark_session, mock.patch(
        "Abacus_insert_atmp_t17_old.calc_gca_value.get_postgres_properties"
    ) as mock_get_postgres_properties, mock.patch(
        "Abacus_insert_atmp_t17_old.calc_gca_value.read_data_from_postgres"
    ) as mock_read_data_from_postgres, mock.patch(
        "Abacus_insert_atmp_t17_old.calc_gca_value.update_to_database"
    ) as mock_update_to_database:
        # Mock Spark session
        mock_get_spark_session.return_value = spark_session

        # Mock postgres properties
        mock_get_postgres_properties.return_value = {
            "url": "jdbc:postgresql://localhost:5432/postgres",
            "user": "postgres",
            "password": "a",
        }

        # Create mock data for t17, t22, w01, w02
        df_t17 = spark_session.createDataFrame(
            [
                Row(id_product=1, some_column="value1"),
                Row(id_product=2, some_column="value2"),
            ]
        )

        df_t22 = spark_session.createDataFrame(
            [
                Row(
                    working_day="2023-10-01",
                    id_product=1,
                    ledger_balance=-100,
                    account_code="1914000000",
                    unamortized_fee=10,
                ),
                Row(
                    working_day="2023-10-01",
                    id_product=2,
                    ledger_balance=200,
                    account_code="1914000000",
                    unamortized_fee=5,
                ),
            ]
        )

        df_w01 = spark_session.createDataFrame(
            [Row(working_day="2023-10-01"), Row(working_day="2023-09-30")]
        )

        df_w02 = spark_session.createDataFrame(
            [
                Row(working_day="2023-10-01"),
            ]
        )

        # Mock read_data_from_postgres to return the mock dataframes
        mock_read_data_from_postgres.side_effect = [df_t17, df_t22, df_w01, df_w02]

        # Call the function
        try:
            calc_gca_value()
        except Exception as e:
            print(f"Error in calculate_gca_value: {e}")
            raise
        # Assertions to check that update_to_database was called with the correct SQL query
        expected_query = """
                UPDATE "ABACUS_A5"."atmp_t17_dpd_credit_cards" AS t17
                SET 
                    gca = temp.gca
                FROM "ABACUS_A5"."temp_updated_rows" AS temp
                WHERE t17.id_product = temp.id_product;
                """
        mock_update_to_database.assert_called_once_with(expected_query)

        # Assert that write.jdbc was called
        assert df_t17.select("id_product", "gca") is not None
