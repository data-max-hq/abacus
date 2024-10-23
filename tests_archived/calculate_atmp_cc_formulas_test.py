import sys
import os

# Add the parent directory to the system path for module imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.abacus_insert_atmp_t17.calculate_atmp_cc_formulas import (
    calculate_atmp_cc_formulas,
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
def mock_calculate_cc_dpd_ho():
    with patch("your_module_name.calculate_cc_dpd_ho") as mock:
        yield mock


@pytest.fixture
def mock_calculate_cc_dpd_ho2():
    with patch("your_module_name.calculate_cc_dpd_ho2") as mock:
        yield mock


@pytest.fixture
def mock_func_maxdpd_month():
    with patch("Functions_old.func_maxdpd_month.func_maxdpd_month") as mock:
        yield mock


@pytest.fixture
def mock_calculate_cc_default():
    with patch("Abacus_insert_default_old.calculate_cc_default") as mock:
        yield mock


@pytest.fixture
def mock_calculate_cc_monthly_recovery():
    with patch("Abacus_insert_default_old.calculate_cc_monthly_recovery") as mock:
        yield mock


@pytest.fixture
def mock_insert_default_events_cc():
    with patch("Abacus_insert_default_event_old.insert_default_events_cc") as mock:
        yield mock


@pytest.fixture
def mock_do_calculations_unwinding_cc():
    with patch("Abacus_unwinding_old.do_calculations_unwinding_cc") as mock:
        yield mock


@pytest.fixture
def mock_calc_gca_value():
    with patch("your_module_name.calc_gca_value") as mock:
        yield mock


@pytest.fixture
def mock_calc_grossinterest_mtd():
    with patch("your_module_name.calc_grossinterest_mtd") as mock:
        yield mock


def test_calculate_atmp_cc_formulas(
    mock_get_spark_session,
    mock_get_postgres_properties,
    mock_calculate_cc_dpd_ho,
    mock_calculate_cc_dpd_ho2,
    mock_func_maxdpd_month,
    mock_calculate_cc_default,
    mock_calculate_cc_monthly_recovery,
    mock_insert_default_events_cc,
    mock_do_calculations_unwinding_cc,
    mock_calc_gca_value,
    mock_calc_grossinterest_mtd,
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

    # Mock DataFrame joins and transformations
    df_mock.join.return_value = df_updated_mock
    df_updated_mock.withColumn.return_value = df_updated_mock

    # Mock write operation
    df_updated_mock.write.jdbc = MagicMock()

    # Call the function to test
    calculate_atmp_cc_formulas()

    # Assertions for DataFrame reads
    mock_spark_session.read.jdbc.assert_any_call(
        table="atmp_t17_dpd_credit_cards", properties=mock_postgres_properties
    )
    mock_spark_session.read.jdbc.assert_any_call(
        table="t01_customer", properties=mock_postgres_properties
    )
    mock_spark_session.read.jdbc.assert_any_call(
        table="atmp_customer", properties=mock_postgres_properties
    )

    # Assertions for joins and transformations
    df_mock.join.assert_called_with(
        df_mock, df_mock.nrp_customer == df_mock.customer_number, "left"
    )
    df_mock.withColumn.assert_called()

    # Assertions for write operations
    df_updated_mock.write.jdbc.assert_any_call(
        table="atmp_t17_dpd_credit_cards",
        properties=mock_postgres_properties,
        mode="overwrite",
    )

    # Assertions for external function calls
    mock_calculate_cc_dpd_ho.assert_called_once()
    mock_calculate_cc_dpd_ho2.assert_called_once()
    mock_func_maxdpd_month.assert_called()
    mock_calculate_cc_default.assert_called_once()
    mock_calculate_cc_monthly_recovery.assert_called_once()
    mock_insert_default_events_cc.assert_called_once()
    mock_do_calculations_unwinding_cc.assert_called_once()
    mock_calc_gca_value.assert_called_once()
    mock_calc_grossinterest_mtd.assert_called_once()

    # Ensure Spark session is stopped
    mock_spark_session.stop.assert_called_once()
