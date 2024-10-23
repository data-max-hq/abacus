from pyspark.sql.functions import col, when, lit

from db_connection_util import read_data_from_postgres


def func_date_since_pd_ho2_cc(spark, schema_name, df):
    # Read the configuration table
    df_config = read_data_from_postgres(spark, "tc08_configuration_table", schema_name)

    # Join with the configuration table
    df_with_config = df.join(
        df_config, df.retail_nonretail_indicator == df_config.segment_type, "left"
    )

    # Add the v_mindate column
    df_result = df_with_config.withColumn(
        "v_mindate",
        when(
            (col("amount_past_due") / col("EUR_RATE") > col("pd_threshold2"))
            & (col("p_date_since_pd_ho2").isNotNull()),
            col("p_date_since_pd_ho2"),
        )
        .when(
            (col("amount_past_due") / col("EUR_RATE") > col("pd_threshold2"))
            & (col("p_date_since_pd_ho2").isNull()),
            col("t17.working_day"),
        )
        .otherwise(lit(None)),
    )

    return df_result.select(col("t17.*"), col("v_mindate"))
