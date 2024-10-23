import sys
import os

# Add the parent directory to the system path for module imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.abacus_insert_atmp_t17.check_product_type import check_product_type
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


def test_check_product_type(
    mock_get_spark_session, mock_get_postgres_properties, mock_read_data_from_postgres
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
    os.environ["PYSPARK_PYTHON"] = "python"
    mock_get_postgres_properties.return_value = mock_postgres_properties

    # Mock the DataFrames
    df_source_mock = MagicMock()
    df_target_mock = MagicMock()
    df_to_insert_mock = MagicMock()
    mock_read_data_from_postgres.side_effect = [
        df_source_mock,  # For atmp_t17_dpd_credit_cards
        df_target_mock,  # For t22_card_balance
        df_to_insert_mock,  # For w01_working_day  # For t02_customer_product
    ]

    # Set the return values for read.jdbc
    mock_spark_session.read.jdbc.side_effect = [df_source_mock, df_target_mock]

    # Prepare the source mock DataFrame
    df_source_mock.select.return_value.distinct.return_value = df_source_mock
    df_source_mock.alias.return_value = df_source_mock  # Keep it the same for chaining

    # Mock the target DataFrame join operation
    df_to_insert_mock.alias.return_value = df_to_insert_mock
    df_to_insert_mock.write.jdbc = MagicMock()  # Mock write method

    # Call the function to test
    result = check_product_type()
    # Assertions
    assert result is True
