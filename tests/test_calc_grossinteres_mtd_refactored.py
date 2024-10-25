import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from faker import Faker
from warnings import filterwarnings
from ..src.insert_atmp_t17.calc_gross_interest_mtd_refactored import GrossInterestMtdEtl

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
    dpd_credit_cards_df = generate_fake_dpd_credit_cards_df(spark)

    # When
    gross_interest_etl = GrossInterestMtdEtl()
    result = gross_interest_etl.transform(dpd_credit_cards_df)
    df = result[0]

    # Then
    assert 3 == df.count()  # Check the number of records
    assert {"id_product", "gross_interest_mtd_lcy", "gross_interest_mtd_ocy"} == set(
        df.columns
    )
    assert (
        df.filter(df.gross_interest_mtd_lcy.isNull()).count() == 0
    )  # Ensure no nulls in the result
    assert (
        df.filter(df.gross_interest_mtd_ocy.isNull()).count() == 0
    )  # Ensure no nulls in the result


def generate_fake_dpd_credit_cards_df(spark: SparkSession):
    return spark.createDataFrame(
        [
            Row(
                id_product="123456",
                working_day="2023-10-01",  # Should trigger the first condition
                gross_interest_daily_lcy=100.0,
                gross_interest_daily_ocy=50.0,
                gross_interest_mtd_lcy=0.0,
                gross_interest_mtd_ocy=0.0,
            ),
            Row(
                id_product="123456",
                working_day="2023-10-02",  # Should sum with previous day
                gross_interest_daily_lcy=200.0,
                gross_interest_daily_ocy=100.0,
                gross_interest_mtd_lcy=100.0,
                gross_interest_mtd_ocy=50.0,
            ),
            Row(
                id_product="123456",
                working_day="2023-10-03",  # Should sum with previous day
                gross_interest_daily_lcy=150.0,
                gross_interest_daily_ocy=75.0,
                gross_interest_mtd_lcy=300.0,
                gross_interest_mtd_ocy=150.0,
            ),
        ]
    )
