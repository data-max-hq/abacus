from pyspark.sql.functions import col, when, expr


def func_maxdpd_month(p_maxdpd_month, dpd_ho, working_day):
    """
    PySpark implementation of the Oracle function ABACUS_A5.func_maxdpd_month.

    Parameters:
    - p_maxdpd_month: The previous month's max days past due.
    - dpd_ho: The current days past due.
    - working_day: The working day of the record.

    Returns:
    - The calculated max days past due for the current month.
    """
    return (
        when(expr("to_char(working_day, 'DD')") == "01", dpd_ho)
        .otherwise(
            when(col("p_maxdpd_month").isNotNull(), col("p_maxdpd_month"))
            .otherwise(0)
            .cast("double")
        )
        .when(col("dpd_ho") > col("p_maxdpd_month"), col("dpd_ho"))
        .otherwise(col("p_maxdpd_month"))
    )
