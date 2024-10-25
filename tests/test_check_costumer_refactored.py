import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col
from ..src.insert_atmp_t17.check_costumer_refactored import CustomerEtl
from faker import Faker
from warnings import filterwarnings

filterwarnings("ignore")
fake = Faker()


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local")
        .config("spark.driver.host", "localhost")
        .config("spark.port.maxRetries", 100)
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_transform(spark):
    # Given
    df_t01 = generate_fake_t01_customer_df(spark)
    df_t17 = generate_fake_atmp_t17_dpd_credit_cards_df(spark)

    # When
    customer_etl = CustomerEtl()
    df_new_customers = customer_etl.transform(df_t01, df_t17)

    # Then
    assert 2 == df_new_customers.count()  # Expecting 2 new customer
    assert {"nrp_customer"} == set(df_new_customers.columns)
    assert (
        df_new_customers.filter(col("nrp_customer").isNull()).count() == 0
    )  # Ensure no nulls in the result


def generate_fake_t01_customer_df(spark):
    return spark.createDataFrame(
        [
            Row(nrp_customer="123456"),  # Existing customer
            Row(nrp_customer="654321"),  # Existing customer
        ]
    )


def generate_fake_atmp_t17_dpd_credit_cards_df(spark: SparkSession):
    return spark.createDataFrame(
        [
            Row(customer_number="111111"),  # New customer
            Row(customer_number="123456"),  # Existing customer
            Row(customer_number="222222"),  # New customer
        ]
    )
