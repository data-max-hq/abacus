import sys
import os

from pyspark.sql.functions import col

# Add the parent directory to the system path for module imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.abacus_insert_atmp_t17.calc_grossinteres_mtd import (
    calc_grossinterest_mtd,
)  # Replace with the actual module name
import pytest
from unittest.mock import patch, MagicMock


# Mocking the dependencies
@pytest.fixture
def mock_get_spark_session():
    with patch("db_connection_util.get_spark_session") as mock:
        yield mock


@pytest.fixture
def mock_get_postgres_properties():
    with patch("db_connection_util.get_postgres_properties") as mock:
        yield mock


@pytest.fixture
def mock_func_calc_grossinterest_mtd():
    with patch(
        "Functions_old.func_calc_grossinterest_mtd.func_calc_grossinterest_mtd"
    ) as mock:
        yield mock


def test_calc_grossinterest_mtd(
    mock_get_spark_session,
    mock_get_postgres_properties,
    mock_func_calc_grossinterest_mtd,
):
    # Mock the return values of the functions
    mock_spark_session = MagicMock()
    mock_get_spark_session.return_value = mock_spark_session

    mock_postgres_properties = {
        "url": "jdbc:postgresql://localhost:5432/postgres",
        "user": "postgres",
        "password": "United#states2023!",
        "driver": "org.postgresql.Driver",
    }
    mock_get_postgres_properties.return_value = mock_postgres_properties

    # Mock DataFrames
    df_mock = MagicMock()
    df_updated_mock = MagicMock()

    # Mock JDBC reads
    mock_spark_session.read.jdbc.return_value = df_mock

    # Mock the func_calc_grossinterest_mtd function
    mock_func_calc_grossinterest_mtd.side_effect = (
        lambda col1, col2, col3: col1 + col2
    )  # Example logic

    # Mock withColumn
    df_mock.withColumn.side_effect = lambda col_name, col_value: df_updated_mock

    # Mock write operation
    df_updated_mock.write.jdbc = MagicMock()

    # Call the function to test
    calc_grossinterest_mtd()

    # Assertions
    mock_spark_session.read.jdbc.assert_called_once_with(
        table="atmp_t17_dpd_credit_cards", properties=mock_postgres_properties
    )

    df_mock.withColumn.assert_any_call(
        "gross_interest_mtd_lcy",
        mock_func_calc_grossinterest_mtd(
            col("gross_interest_mtd_lcy"),
            col("gross_interest_daily_lcy"),
            col("working_day"),
        ),
    )

    df_mock.withColumn.assert_any_call(
        "gross_interest_mtd_ocy",
        mock_func_calc_grossinterest_mtd(
            col("gross_interest_mtd_ocy"),
            col("gross_interest_daily_ocy"),
            col("working_day"),
        ),
    )

    df_updated_mock.write.jdbc.assert_called_once_with(
        table="atmp_t17_dpd_credit_cards",
        properties=mock_postgres_properties,
        mode="overwrite",
    )

    # Ensure Spark session is stopped
    mock_spark_session.stop.assert_called_once()
