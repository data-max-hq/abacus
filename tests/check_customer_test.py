import sys
import os

# Add the parent directory to the system path for module imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.abacus_insert_atmp_t17.check_customer import (
    check_customer,
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


def test_check_customer_execution(
    mock_get_spark_session, mock_get_postgres_properties, mock_read_data_from_postgres
):
    # Mock the return values of the functions
    mock_get_spark_session.return_value = MagicMock()  # Create a mock Spark session
    mock_get_postgres_properties.return_value = {
        "url": "jdbc:postgresql://localhost:5432/postgres",
        "user": "postgres",
        "password": "United#states2023!",
        "driver": "org.postgresql.Driver",
    }
    os.environ["PYSPARK_PYTHON"] = "python"
    # Create mock DataFrames
    df_source_mock = MagicMock()
    df_target_mock = MagicMock()
    mock_read_data_from_postgres.side_effect = [df_source_mock, df_target_mock]

    # Call the function to test
    result = check_customer()

    # Assertions
    assert result is True  # Assuming your function returns True on success
