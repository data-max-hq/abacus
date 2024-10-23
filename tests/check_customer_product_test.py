import sys
import os

# Add the parent directory to the system path for module imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.abacus_insert_atmp_t17.check_customer_product import (
    check_customer_product,
)  # Adjust the import based on your actual module structure
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
def mock_read_data_from_postgres():
    with patch("db_connection_util.read_data_from_postgres") as mock:
        yield mock


def test_check_customer_product(
    mock_get_spark_session, mock_get_postgres_properties, mock_read_data_from_postgres
):
    # Mock the return values of the functions
    mock_get_spark_session.return_value = MagicMock()  # Create a mock Spark session
    mock_get_postgres_properties.return_value = {
        "url": "jdbc:postgresql://localhost:5432/postgres",
        "user": "postgres",
        "password": "a",
        "driver": "org.postgresql.Driver",
    }
    os.environ["PYSPARK_PYTHON"] = "python"
    # Create mock DataFrames
    df_credit_cards_mock = MagicMock()
    df_card_balance_mock = MagicMock()
    df_working_day_mock = MagicMock()
    df_target_mock = MagicMock()

    # Set the side effects for read_data_from_postgres
    mock_read_data_from_postgres.side_effect = [
        df_credit_cards_mock,  # For atmp_t17_dpd_credit_cards
        df_card_balance_mock,  # For t22_card_balance
        df_working_day_mock,  # For w01_working_day
        df_target_mock,  # For t02_customer_product
    ]

    # Mock the behaviors of DataFrames
    df_working_day_mock.agg.return_value.collect.return_value = [
        (1,)
    ]  # Mock max working day return
    df_card_balance_mock.filter.return_value = (
        df_card_balance_mock  # Keep it the same for chaining
    )
    df_credit_cards_mock.alias.return_value = (
        df_credit_cards_mock  # Keep it the same for chaining
    )
    df_target_mock.alias.return_value = df_target_mock  # Keep it the same for chaining
    df_target_mock.write.jdbc = MagicMock()  # Mock write method

    # Call the function to test
    result = check_customer_product()

    # Assertions
    assert result is True  # Assuming your function returns True on success
