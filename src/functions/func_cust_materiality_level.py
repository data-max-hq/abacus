from pyspark.sql import functions as F
from pyspark.sql.functions import lit, col


def func_cust_materiality_level(df, config_df):
    """
    This function replaces the UDF logic for calculating customer materiality level.
    It calculates the materiality based on the segment type and thresholds from the config table.
    """
    # Join the input dataframe with the configuration dataframe based on segment_type
    df = df.join(
        config_df, df["retail_nonretail_indicator"] == config_df["segment_type"], "left"
    )
    vnew_dod = "1"

    # Apply the logic to calculate materiality based on the thresholds
    df = df.withColumn(
        "materiality_level",
        F.when(
            # Only calculate materiality if thresholds are not null (i.e., valid match found)
            (df["pd_threshold"].isNotNull())
            & (df["percentage_threshold"].isNotNull())
            & (df["t22_ledger_balance"].isNotNull()),
            F.when(
                vnew_dod == lit("1"),  # Hardcode '1' for vnew_dod as in SQL
                F.when(
                    df["pd_threshold"]
                    >= df["percentage_threshold"] / 100 * df["t22_ledger_balance"],
                    F.col("pd_threshold"),
                ).otherwise(
                    df["percentage_threshold"] / 100 * df["t22_ledger_balance"]
                ),
            ).otherwise(  # Non-new delinquency calculation (for vnew_dod != '1')
                F.when(
                    df["pd_threshold2"]
                    >= df["percentage_threshold2"] / 100 * df["t22_ledger_balance"],
                    F.col("pd_threshold2"),
                ).otherwise(
                    df["percentage_threshold2"] / 100 * df["t22_ledger_balance"]
                )
            ),
        ).otherwise(col("materiality_level")),
    )

    return df
