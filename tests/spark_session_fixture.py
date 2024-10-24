import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.master("local")
        .config("spark.driver.host", "localhost")
        .getOrCreate()
    )

    yield spark
    spark.stop()
