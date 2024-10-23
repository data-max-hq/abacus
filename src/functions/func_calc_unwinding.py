from pyspark.sql import functions as F
from src.functions.func_year_frac import func_year_frac


def func_calc_unwinding(df, max_working_day):
    """
    Replaces the func_calc_unwinding UDF with equivalent PySpark SQL functions.
    """
    # Apply the year fraction calculation using the refactored func_year_frac_pyspark
    df = func_year_frac(df, "p_closing_day", "closing_day", "calculation_basis")

    # Calculate unwinding using PySpark SQL functions
    return df.withColumn(
        "unwinding_daily_lcy",
        F.when(
            (F.col("product_group") == "CC")
            & (F.col("working_day") == F.lit(max_working_day)),
            F.greatest(F.col("gca_all") - F.col("provision_fund"), F.lit(0))
            * (F.pow(1 + F.col("interest_rate") / 100, F.col("year_frac_result")) - 1),
        ).otherwise(F.col("unwinding_daily_lcy")),
    )
