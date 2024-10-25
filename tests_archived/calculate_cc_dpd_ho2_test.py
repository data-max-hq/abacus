from unittest.mock import patch, MagicMock
import pytest

from src.insert_atmp_t17_deprecated.calculate_cc_dpd_ho2 import calculate_cc_dpd_ho2


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


@pytest.fixture
def mock_psycopg2_connect():
    with patch("psycopg2.connect") as mock:
        yield mock


def test_calculate_cc_dpd_ho2(
    mock_get_spark_session,
    mock_get_postgres_properties,
    mock_read_data_from_postgres,
    mock_psycopg2_connect,
):
    # Mock the return values of the functions
    mock_spark_session = MagicMock()
    mock_get_spark_session.return_value = (
        mock_spark_session  # Create a mock Spark session
    )

    mock_get_postgres_properties.return_value = {
        "url": "jdbc:postgresql://localhost:5432/postgres",
        "user": "postgres",
        "password": "United#states2023!",
        "driver": "org.postgresql.Driver",
    }

    # Create mock DataFrames for the reads
    df_t17_mock = MagicMock()
    df_t22_mock = MagicMock()
    df_t20_mock = MagicMock()
    df_t01_mock = MagicMock()
    df_w01_mock = MagicMock()

    # Set up the side effects for read_data_from_postgres
    mock_read_data_from_postgres.side_effect = [
        df_t17_mock,
        df_t22_mock,
        df_t20_mock,
        df_t01_mock,
        df_w01_mock,
    ]

    # Mock the behavior of psycopg2.connect
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_psycopg2_connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    # Call the function to test
    calculate_cc_dpd_ho2()

    # Assertions
    mock_get_spark_session.assert_called_once()  # Check if Spark session was requested
    assert (
        mock_read_data_from_postgres.call_count == 5
    )  # We expect to read 5 different tables
    mock_psycopg2_connect.assert_called_once()  # Ensure the connection is attempted

    # Check if the cursor executes the expected update query
    expected_update_query = """
    UPDATE "ABACUS_A5"."atmp_t17_dpd_credit_cards" AS t17
    SET date_since_pd_ho2 = temp.date_since_pd_ho2,
        dpd_ho2 = temp.dpd_ho2
    FROM "ABACUS_A5"."temp_updated_rows" AS temp
    WHERE t17.id_product = temp.id_product;
    """
    mock_cursor.execute.assert_called_with(expected_update_query)

    # Ensure temp_updated_rows is dropped
    mock_cursor.execute.assert_any_call(
        """ DROP TABLE "ABACUS_A5"."temp_updated_rows" """
    )

    # Check if commits are called
    assert (
        mock_conn.commit.call_count == 2
    )  # One for the update and one for dropping the temp table

    # Check if the connection is closed
    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()
