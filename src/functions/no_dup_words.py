from pyspark.sql.functions import col, split, array_distinct, concat_ws, when


def no_dup_words(df):
    """
    Applies the no-duplicate-words logic conditionally when default_indicator == 1.
    :param df: Input DataFrame
    :return: DataFrame with the updated default_reason column
    """
    return df.withColumn(
        "default_reason",
        when(
            col("default_indicator") == 1,
            concat_ws(",", array_distinct(split(col("default_reason"), ","))),
        ).otherwise(col("default_reason")),
    )
