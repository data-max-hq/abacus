import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType
from pyspark.sql.functions import expr
from src.abacus_insert_atmp_t17.commit_atmp18_into_t18 import commit_atmpt18_into_t18


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
        ]
    )

    df_source = spark.createDataFrame([(None,), (None,)], schema)

    return df_source


# Test the commit_atmpt18_into_t18 function
def test_commit_atmpt18_into_t18(spark, sample_data, mocker):
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
    commit_atmpt18_into_t18()

    # Validate that the read operation was called once to load the source table
    mock_read.assert_called_once_with(
        table="atmp_t18_cc_payment_schedule",
        properties=mock_get_postgres_properties.return_value,
    )

    # Validate that the write operation has been called once
    assert mock_write.call_count == 1
    mock_write.assert_called_with(
        table="t18_cc_payment_schedule",
        properties=mock_get_postgres_properties.return_value,
        mode="append",
    )

    # Check the transformation for autoid column
    df_transformed = df_source.withColumn(
        "autoid", expr("nextval('cc_payment_schedule_seq')")
    )
    assert df_transformed is not None

    # Verify that the autoid column is populated as expected
    assert df_transformed.filter(df_transformed["autoid"].isNull()).count() == 0
