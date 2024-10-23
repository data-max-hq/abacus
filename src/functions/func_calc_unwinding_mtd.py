from pyspark.sql import functions as F


def func_calc_unwinding_mtd(df):
    """
    This function replaces the UDF logic with native PySpark SQL functions to calculate unwinding MTD.
    It calculates MTD (Month-to-Date) unwinding for credit cards, compatible with the Oracle SQL logic.
    """
    return df.withColumn(
        "unwinding_mtd_lcy",
        F.when(
            F.dayofmonth(F.col("working_day")) == 1,  # If first day of the month
            F.coalesce(
                F.col("unwinding_daily_lcy"), F.lit(0)
            ),  # Set MTD to today's unwinding
        ).otherwise(
            F.coalesce(F.col("unwinding_daily_lcy"), F.lit(0))
            + F.coalesce(F.col("unwinding_mtd_lcy"), F.lit(0))  # Accumulate MTD
        ),
    )
