import sys
import os


# Add the parent directory to the system path for module imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.abacus_insert_atmp_t17.isexistingcustomer import (
    isexistingcustomer,
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


def test_isexistingcustomer(mock_get_spark_session, mock_get_postgres_properties):
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

    # Mock the DataFrame
    df_mock = MagicMock()
    mock_spark_session.read.jdbc.return_value = df_mock

    # Define the behavior for the filter and count methods
    df_mock.filter.return_value = df_mock
    df_mock.count.return_value = 1  # Example: return a count of 1 matching row

    # Call the function to test
    customer_number = "217567"
    count = isexistingcustomer(customer_number)

    # Assertions
    assert count == 1  # Check that the returned count is as mocked
