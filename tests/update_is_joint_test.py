import sys
import os

from pyspark.sql.functions import col, when, lit

# Add the parent directory to the system path for module imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.abacus_insert_atmp_t17.update_is_joint import (
    update_is_joint,
)  # Replace with the actual module name
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


def test_update_is_joint(mock_get_spark_session, mock_get_postgres_properties):
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

    # Mock the DataFrames
    df_customer_relations_mock = MagicMock()
    df_card_balance_mock = MagicMock()
    df_working_day_mock = MagicMock()

    mock_spark_session.read.jdbc.side_effect = [
        df_customer_relations_mock,  # For customer_relations
        df_card_balance_mock,  # For t22_card_balance
        df_working_day_mock,  # For w01_working_day
    ]

    # Mock aggregation and collection to get the latest working day
    df_working_day_mock.agg.return_value.collect.return_value = [Row(max="2024-01-01")]

    # Mock filter and join operations
    df_filtered_card_balance_mock = MagicMock()
    df_card_balance_mock.filter.return_value = df_filtered_card_balance_mock
    df_joined_mock = MagicMock()
    df_customer_relations_mock.alias.return_value.join.return_value = df_joined_mock

    # Mock withColumn operations
    df_updated_mock = MagicMock()
    df_joined_mock.withColumn.side_effect = lambda *args, **kwargs: df_updated_mock

    # Mock write operation
    df_updated_mock.write.jdbc = MagicMock()

    # Call the function to test
    update_is_joint()

    # Assertions
    mock_spark_session.read.jdbc.assert_any_call(
        table="customer_relations", properties=mock_postgres_properties
    )
    mock_spark_session.read.jdbc.assert_any_call(
        table="t22_card_balance", properties=mock_postgres_properties
    )
    mock_spark_session.read.jdbc.assert_any_call(
        table="w01_working_day", properties=mock_postgres_properties
    )

    df_card_balance_mock.filter.assert_called_once_with(
        col("working_day") == "2024-01-01"
    )
    df_customer_relations_mock.alias.assert_called_once()
    df_joined_mock.withColumn.assert_any_call(
        "is_joint",
        when(
            (col("is_joint_micro") == "1")
            & (col("application_role_code") == "BORROWER")
            & (col("product_type") != "LOAN"),
            lit("1"),
        ).otherwise(col("is_joint")),
    )
    df_joined_mock.withColumn.assert_any_call(
        "joint_id",
        when(
            (col("is_joint_micro") == "1")
            & (col("application_role_code") == "BORROWER")
            & (col("product_type") != "LOAN"),
            col("joint_group"),
        ).otherwise(col("joint_id")),
    )

    df_updated_mock.write.jdbc.assert_called_once_with(
        table="atmp_t17_dpd_credit_cards",
        properties=mock_postgres_properties,
        mode="overwrite",
    )

    # Ensure Spark session is stopped
    mock_spark_session.stop.assert_called_once()
