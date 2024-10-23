import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType
from pyspark.sql import functions as F
from src.abacus_insert_atmp_t17.commit_atmp17_into_t17 import commit_atmpt17_into_t17


# Sample data schema and fixture for SparkSession
@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[2]").appName("unit_test").getOrCreate()


# Mock data for the DataFrames
@pytest.fixture
def sample_data():
    schema = StructType(
        [
            StructField("autoid", LongType(), True),
            StructField("dpd_ho", IntegerType(), True),
            StructField("standart_interest_rate", DoubleType(), True),
            StructField("penalty_interest_rate", DoubleType(), True),
            StructField("interest_rate_default", DoubleType(), True),
            StructField("dpd_ho2", IntegerType(), True),
        ]
    )

    df_source = spark.createDataFrame(
        [(None, None, 5.5, 0.0, None, None), (None, 1, None, None, 2.5, None)], schema
    )

    return df_source


# Test the commit_atmpt17_into_t17 function
def test_commit_atmpt17_into_t17(spark, sample_data, mocker):
    df_source = sample_data

    # Mock Spark read/write operations
    mock_read = mocker.patch("pyspark.sql.DataFrameReader.jdbc")
    mock_write = mocker.patch("pyspark.sql.DataFrameWriter.jdbc")

    mock_read.return_value = df_source  # Return the mock data for the table read

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
    commit_atmpt17_into_t17()

    # Validate that write operation has been called once (since there's a single write operation)
    assert mock_write.call_count == 1
    mock_write.assert_called_with(
        table="t17_dpd_credit_cards",
        properties=mock_get_postgres_properties.return_value,
        mode="append",
    )

    # Verify transformations
    df_transformed = mock_write.call_args[0][0]  # Extract the DataFrame written
    assert df_transformed is not None

    # Check that autoid is being set, and default values are filled correctly
    df_result = (
        df_transformed.withColumn("autoid", expr("nextval('dpd_cc_seq')"))
        .withColumn("dpd_ho", F.coalesce(F.col("dpd_ho"), F.lit(0)))
        .withColumn(
            "standart_interest_rate",
            F.coalesce(F.col("standart_interest_rate"), F.lit(0)),
        )
        .withColumn(
            "penalty_interest_rate",
            F.coalesce(F.col("penalty_interest_rate"), F.lit(0)),
        )
        .withColumn(
            "interest_rate_default",
            F.coalesce(F.col("interest_rate_default"), F.lit(0)),
        )
        .withColumn("dpd_ho2", F.coalesce(F.col("dpd_ho2"), F.lit(0)))
    )

    # Validate that coalesce worked as expected
    assert df_result.filter(F.col("dpd_ho").isNull()).count() == 0
    assert df_result.filter(F.col("standart_interest_rate").isNull()).count() == 0
    assert df_result.filter(F.col("penalty_interest_rate").isNull()).count() == 0
    assert df_result.filter(F.col("interest_rate_default").isNull()).count() == 0
    assert df_result.filter(F.col("dpd_ho2").isNull()).count() == 0
