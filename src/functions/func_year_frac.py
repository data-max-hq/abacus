from pyspark.sql import functions as F
from pyspark.sql.functions import last_day


def func_year_frac(df, start_date_col, end_date_col, basis_col):
    """
    This function calculates the year fraction based on the start date, end date, and calculation basis using PySpark SQL functions.
    """

    # Adjust start and end dates based on the basis
    df = df.withColumn("v_start_date", F.dayofmonth(F.col(start_date_col)))
    df = df.withColumn("v_end_date", F.dayofmonth(F.col(end_date_col)))

    # Handle basis 3 and 7 (30/360 or similar bases)
    df = df.withColumn(
        "v_start_date",
        F.when(
            (F.col(basis_col).isin(3, 7)) & (F.dayofmonth(F.col(start_date_col)) == 31),
            F.lit(30),
        ).otherwise(F.col("v_start_date")),
    )
    df = df.withColumn(
        "v_end_date",
        F.when(
            (F.col(basis_col).isin(3, 7)) & (F.dayofmonth(F.col(end_date_col)) == 31),
            F.lit(30),
        ).otherwise(F.col("v_end_date")),
    )

    # Calculate the numerator for basis 3 and 7
    df = df.withColumn(
        "v_numerator",
        F.when(
            F.col(basis_col).isin(3, 7),
            (F.year(F.col(end_date_col)) - F.year(F.col(start_date_col))) * 360
            + (F.month(F.col(end_date_col)) - F.month(F.col(start_date_col))) * 30
            + (F.col("v_end_date") - F.col("v_start_date")),
        ).otherwise(F.lit(0)),
    )

    # Adjustments for special cases in basis 3 and 7
    df = df.withColumn(
        "v_numerator",
        F.when(
            (F.col(basis_col).isin(3, 7))
            & (F.dayofmonth(F.col(end_date_col)) == 1)
            & (F.dayofmonth(F.col(start_date_col)) < 31)
            & (
                F.dayofmonth(last_day(F.col(start_date_col))) == 31
            ),  # Corrected comparison to extract day from last_day
            F.datediff(
                F.col(end_date_col), F.expr("INTERVAL 1 DAY") + F.col(start_date_col)
            ),
        ).otherwise(F.col("v_numerator")),
    )

    # Denominator and numerator for other bases
    df = df.withColumn(
        "v_denominator",
        F.when(F.col(basis_col) == 6, F.lit(366))
        .  # ACT/366
        when(F.col(basis_col).isin(2, 5), F.lit(360))
        .  # ACT/360
        when(F.col(basis_col).isin(1, 4), F.lit(365))
        .  # ACT/365
        otherwise(F.lit(360)),  # Default 30/360
    )

    # Numerator for ACT-based calculations (days between dates)
    df = df.withColumn(
        "v_numerator",
        F.when(
            F.col(basis_col).isin(1, 2, 4, 5, 6),
            F.datediff(F.col(end_date_col), F.col(start_date_col)),
        ).otherwise(F.col("v_numerator")),
    )

    # Final result (year fraction)
    df = df.withColumn(
        "year_frac_result", F.round(F.col("v_numerator") / F.col("v_denominator"), 9)
    )

    return df
