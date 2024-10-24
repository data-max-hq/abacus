from faker import Faker
from pyspark.sql import Row
from pyspark.sql import SparkSession

from src.insert_atmp_t17.calc_gca_value_refactored import GcsEtl

fake = Faker()


def test_transform(spark_session: SparkSession) -> None:
    # Given
    product_id = "123456"
    working_date_df = generate_fake_working_date_df(spark_session=spark_session)
    cc_working_date_df = generate_fake_cc_working_day_df(spark_session=spark_session)
    card_balance = generate_fake_card_balance_df(
        spark_session=spark_session, id_product=product_id
    )
    dpd_credit_cards = generate_fake_dpd_credit_cards_df(
        spark_session=spark_session, id_product=product_id
    )

    # When
    gcs_etl = GcsEtl()
    result = gcs_etl.transform(
        dpd_credit_cards_df=dpd_credit_cards,
        card_balance_df=card_balance,
        working_day_df=working_date_df,
        cc_working_day_df=cc_working_date_df,
    )
    df = result[0]

    # Then
    assert 3 == df.count()
    assert {"id_product", "gca"} == set(df.columns)
    assert 2 == df.select("id_product").distinct().count()


def generate_fake_working_date_df(spark_session):
    return spark_session.createDataFrame(
        data=[
            Row(
                working_day="2009-05-21",
                next_working_day="2009-05-22",
                closing_date="2009-05-21",
            ),
            Row(
                working_day="2009-05-27",
                next_working_day="2009-05-28",
                closing_date="2009-05-27",
            ),
            Row(
                working_day="2009-06-03",
                next_working_day="2009-06-04",
                closing_date="2009-06-03",
            ),
            Row(
                working_day="2009-06-10",
                next_working_day="2009-06-11",
                closing_date="2009-06-10",
            ),
            Row(
                working_day="2009-06-15",
                next_working_day="2009-06-16",
                closing_date="2009-06-15",
            ),
            Row(
                working_day="2013-01-20",
                next_working_day="2009-06-22",
                closing_date="2009-06-21",
            ),
        ]
    )


def generate_fake_cc_working_day_df(spark_session):
    return spark_session.createDataFrame(
        [
            Row(working_day="2013-01-12"),
            Row(working_day="2013-01-14"),
            Row(working_day="2013-01-16"),
            Row(working_day="2013-01-18"),
            Row(working_day="2013-01-19"),
            Row(working_day="2013-01-20"),
        ]
    )


def generate_fake_card_balance_df(spark_session: SparkSession, id_product: str):
    return spark_session.createDataFrame(
        [
            Row(
                working_day="2013-01-20",
                account_code=str(fake.random_number(digits=10)),
                ledger_balance=-238.44,
                id_product=id_product,
                unamortized_fee=0,
            ),
            Row(
                working_day="2013-01-20",
                account_code=str(fake.random_number(digits=10)),
                ledger_balance=-2000.3,
                id_product=id_product,
                unamortized_fee=0,
            ),
        ]
    )


def generate_fake_dpd_credit_cards_df(spark_session: SparkSession, id_product: str):
    return spark_session.createDataFrame(
        [
            Row(
                gca=1474.09,
                id_product=id_product,
            ),
            Row(
                gca=200.03,
                id_product="some-other-id-product",
            ),
        ]
    )
