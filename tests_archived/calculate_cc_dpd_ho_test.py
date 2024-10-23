import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)
from src.abacus_insert_atmp_t17.calculate_cc_dpd_ho import calculate_cc_dpd_ho


# Mock data for testing
@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[2]").appName("unit_test").getOrCreate()


@pytest.fixture
def sample_data():
    schema_atmp_t17 = StructType(
        [
            StructField("id_product", IntegerType(), True),
            StructField("retail_nonretail_indicator", StringType(), True),
            StructField("ledger_balance", DoubleType(), True),
            StructField("customer_number", IntegerType(), True),
        ]
    )

    schema_t22 = StructType(
        [
            StructField("id_product", IntegerType(), True),
            StructField("working_day", IntegerType(), True),
            StructField("ACCOUNT_CURRENCY", StringType(), True),
            StructField("amount_past_due", DoubleType(), True),
        ]
    )

    df_atmp_t17 = spark.createDataFrame(
        [(1, "RETAIL", -100.0, 1234), (2, "NONRETAIL", 200.0, 5678)], schema_atmp_t17
    )

    df_t22 = spark.createDataFrame(
        [(1, 20231001, "USD", 500.0), (2, 20231001, "EUR", 0.0)], schema_t22
    )

    return df_atmp_t17, df_t22


# Test function
def test_calculate_cc_dpd_ho(spark, sample_data, mocker):
    df_atmp_t17, df_t22 = sample_data

    # Mock Spark read/write operations
    mock_read = mocker.patch("pyspark.sql.DataFrameReader.jdbc")
    mock_write = mocker.patch("pyspark.sql.DataFrameWriter.jdbc")

    mock_read.side_effect = [df_atmp_t17, df_t22]  # Return the mock data

    # Mock the functions get_spark_session and get_postgres_properties
    mock_get_spark_session = mocker.patch("db_connection_util.get_spark_session")
    mock_get_postgres_properties = mocker.patch(
        "db_connection_util.get_postgres_properties"
    )

    mock_get_spark_session.return_value = spark
    mock_get_postgres_properties.return_value = {
        "url": "jdbc:postgresql://localhost/test",
        "user": "user",
        "password": "password",
    }

    # Call the function to test
    calculate_cc_dpd_ho()

    # Validate that write operation has been called once (for df_final)
    assert mock_write.call_count == 1
    mock_write.assert_called_with(table="atmp_t17_dpd_credit_cards", mode="overwrite")

    # Verify transformations
    df_final = mock_write.call_args[0][
        0
    ]  # Extract the DataFrame written in the final step
    assert df_final is not None
    assert df_final.filter(col("dpd_ho") > 0).count() > 0
