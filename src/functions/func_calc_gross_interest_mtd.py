from pyspark.sql import functions as F


def calc_gross_interest_mtd_sql(df):
    """
    Applies the SQL transformations to calculate the gross_interest_mtd_lcy and gross_interest_mtd_ocy
    based on the gross_interest_daily_* columns and the working_day.

    :param df: Input DataFrame (df_t17)
    :return: DataFrame with updated gross_interest_mtd_lcy and gross_interest_mtd_ocy columns
    """

    df_updated = df.withColumn(
        "gross_interest_mtd_lcy",
        F.when(
            F.date_format(F.col("working_day"), "dd") == "01",
            F.col("gross_interest_daily_lcy"),
        ).otherwise(
            F.col("gross_interest_mtd_lcy") + F.col("gross_interest_daily_lcy")
        ),
    ).withColumn(
        "gross_interest_mtd_ocy",
        F.when(
            F.date_format(F.col("working_day"), "dd") == "01",
            F.col("gross_interest_daily_ocy"),
        ).otherwise(
            F.col("gross_interest_mtd_ocy") + F.col("gross_interest_daily_ocy")
        ),
    )

    return df_updated.select(
        "id_product", "gross_interest_mtd_lcy", "gross_interest_mtd_ocy"
    )
