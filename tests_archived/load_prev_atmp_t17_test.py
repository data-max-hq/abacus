import sys
import os

# Add the parent directory to the system path for module imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.abacus_insert_atmp_t17.load_prev_atmp_t17 import load_prev_atmp_t17
import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import Row


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


def test_load_prev_atmp_t17(
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
    mock_get_postgres_properties.return_value = mock_postgres_properties

    # Mock DataFrames
    df_atmp_t17_mock = MagicMock()
    df_t17_mock = MagicMock()
    df_w02_mock = MagicMock()
    df_merged_mock = MagicMock()
    df_atmp_t17_updated_mock = MagicMock()

    # Mock the read data calls for different tables
    mock_read_data_from_postgres.side_effect = [
        df_atmp_t17_mock,  # atmp_t17_dpd_credit_cards
        df_t17_mock,  # t17_dpd_credit_cards
        df_w02_mock,  # w02_ccworking_day
        df_atmp_t17_updated_mock,  # atmp_t17_dpd_credit_cards (after update)
    ]

    # Mock the max working day calculations
    df_w02_mock.agg.return_value.collect.side_effect = [
        [Row(max_working_day=100)],  # max_working_day
        [Row(max_working_day=99)],  # prev_max_working_day
    ]

    # Mock filtering and joining
    df_t17_mock.filter.return_value = (
        df_t17_mock  # Mock filtering to get previous working day data
    )
    df_atmp_t17_mock.alias.return_value = df_atmp_t17_mock
    df_t17_mock.alias.return_value = df_t17_mock
    df_atmp_t17_mock.join.return_value = df_merged_mock  # Mock the join operation

    # Mock select and withColumn operations
    df_merged_mock.select.return_value = df_merged_mock
    df_merged_mock.withColumn.return_value = (
        df_atmp_t17_updated_mock  # For p_working_day update
    )

    # Mock JDBC write operation
    df_merged_mock.write.jdbc = MagicMock()  # Mock write for the first merged DataFrame
    df_atmp_t17_updated_mock.write.jdbc = (
        MagicMock()
    )  # Mock write for the updated DataFrame

    # Call the function to test
    result = load_prev_atmp_t17()

    # Assertions
    assert result is True
