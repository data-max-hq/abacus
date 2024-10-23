from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    when,
    lit,
    concat,
    add_months,
    substring,
    length,
    instr,
)
from pyspark.sql.types import DateType, StringType
from db_connection_util import (
    get_spark_session,
    get_postgres_properties,
    read_data_from_postgres,
)


def insert_default_events_cc():
    try:
        spark = get_spark_session()
        postgres_properties = get_postgres_properties()
        schema_name = "ABACUS_A5"

        # Load necessary tables
        df_t17 = read_data_from_postgres(
            spark, "atmp_t17_dpd_credit_cards", schema_name
        )
        df_t19 = read_data_from_postgres(spark, "t19_default_events", schema_name)
        df_tc_default_reason = read_data_from_postgres(
            spark, "tc_default_reason", schema_name
        )
        df_w02 = read_data_from_postgres(spark, "w02_ccworking_day", schema_name)

        # Get the working day and next working day
        v_working_day = df_w02.agg(F.max("working_day")).collect()[0][0]
        v_next_working_day = df_w02.agg(F.max("working_day") + 1).collect()[0][0]

        # Delete existing events for the current working day
        df_t19 = df_t19.filter(
            ~(
                (col("event_start_date") == lit(v_working_day))
                & (substring(col("account_id"), 1, 2) == "CC")
                & (length(col("account_id")) == 26)
                & (substring(col("account_id"), 12, 2).isin("18", "19"))
            )
        )

        # Function to create DataFrame for each default event type
        def create_default_event_df(event_type, condition):
            reason_row = (
                df_tc_default_reason.filter(col("reason_desc") == event_type)
                .select("reason_code")
                .first()
            )
            if reason_row is None:
                print(
                    f"Warning: No reason_code found for event type '{event_type}'. Using event type as reason code."
                )
                reason_code = event_type
            else:
                reason_code = reason_row[0]

            return df_t17.filter(condition).select(
                col("customer_number"),
                col("id_product").alias("account_id"),
                lit(reason_code).alias("default_trigger_id"),
                lit(event_type).alias("default_trigger_description"),
                lit(None).cast(StringType()).alias("default_object"),
                col("working_day").alias("event_start_date"),
                lit("ACTIVE").alias("event_status"),
                lit(None).cast(DateType()).alias("probation_end_reset_date"),
                when(
                    col("end_probation_date").isNull(),
                    add_months(lit(v_next_working_day), 3),
                )
                .otherwise(col("end_probation_date"))
                .alias("probation_end_date"),
                lit(None).cast(StringType()).alias("corresponding_default_event_id"),
                concat(
                    F.date_format(col("working_day"), "yyyyMMdd"),
                    col("customer_number"),
                    lit(reason_code),
                ).alias("default_event_id"),
                lit(None).cast(StringType()).alias("autoid"),
            )

        # Create DataFrames for each default event type
        df_dpd = create_default_event_df(
            "DPD",
            (col("default_indicator") == "1")
            & (col("p_dpd_ho") <= 90)
            & (col("dpd_ho") > 90)
            & (col("retail_nonretail_indicator") == "RETAIL"),
        )

        df_contract_termination = create_default_event_df(
            "CONTRACT TERMINATION",
            (
                (col("default_indicator") == "1")
                & (col("p_default_indicator") == "0")
                & (instr(col("default_reason"), "CONTRACT TERMINATION") > 0)
            )
            | (
                (instr(col("default_reason"), "CONTRACT TERMINATION") > 0)
                & (instr(col("p_default_reason"), "CONTRACT TERMINATION") == 0)
            ),
        )

        df_debt_sale = create_default_event_df(
            "DEBT SALE",
            (
                (col("default_indicator") == "1")
                & (col("p_default_indicator") == "0")
                & (instr(col("default_reason"), "DEBT SALE") > 0)
            )
            | (
                (instr(col("default_reason"), "DEBT SALE") > 0)
                & (instr(col("p_default_reason"), "DEBT SALE") == 0)
            ),
        )

        df_foreclosure = create_default_event_df(
            "FORECLOSURE COLLATERAL",
            (
                (col("default_indicator") == "1")
                & (col("p_default_indicator") == "0")
                & (instr(col("default_reason"), "FORECLOSURE COLLATERAL") > 0)
            )
            | (
                (instr(col("default_reason"), "FORECLOSURE COLLATERAL") > 0)
                & (instr(col("p_default_reason"), "FORECLOSURE COLLATERAL") == 0)
            ),
        )

        df_cross_contagion = create_default_event_df(
            "CROSS CONTAGION INDICATOR",
            (
                (col("default_indicator") == "1")
                & (col("p_default_indicator") == "0")
                & (col("default_reason").like("%CROSS CONTAGION INDICATOR"))
            )
            | (
                (col("default_reason").like("%CROSS CONTAGION INDICATOR"))
                & (~col("p_default_reason").like("%CROSS CONTAGION INDICATOR"))
            ),
        )

        df_cross_default = create_default_event_df(
            "CROSS DEFAULT PULLING EFFECT",
            (
                (col("default_indicator") == "1")
                & (col("p_default_indicator") == "0")
                & (col("default_reason").like("%CROSS DEFAULT PULLING EFFECT"))
            )
            | (
                (col("default_reason").like("%CROSS DEFAULT PULLING EFFECT"))
                & (~col("p_default_reason").like("%CROSS DEFAULT PULLING EFFECT"))
            ),
        )

        df_credit_risk = create_default_event_df(
            "CREDIT RISK ADJUSTMENT",
            (
                (col("default_indicator") == "1")
                & (col("p_default_indicator") == "0")
                & (instr(col("default_reason"), "CREDIT RISK ADJUSTMENT") > 0)
            )
            | (
                (instr(col("default_reason"), "CREDIT RISK ADJUSTMENT") > 0)
                & (instr(col("p_default_reason"), "CREDIT RISK ADJUSTMENT") == 0)
            ),
        )

        df_non_accrued = create_default_event_df(
            "NON ACCRUED STATUS",
            (
                (col("default_indicator") == "1")
                & (col("p_default_indicator") == "0")
                & (instr(col("default_reason"), "NON ACCRUED STATUS") > 0)
            )
            | (
                (instr(col("default_reason"), "NON ACCRUED STATUS") > 0)
                & (instr(col("p_default_reason"), "NON ACCRUED STATUS") == 0)
            ),
        )

        df_breach_covenants = create_default_event_df(
            "BREACH OF COVENANTS",
            (
                (col("default_indicator") == "1")
                & (col("p_default_indicator") == "0")
                & (instr(col("default_reason"), "BREACH OF COVENANTS") > 0)
            )
            | (
                (instr(col("default_reason"), "BREACH OF COVENANTS") > 0)
                & (instr(col("p_default_reason"), "BREACH OF COVENANTS") == 0)
            ),
        )

        # Union all default event DataFrames
        df_all_events = (
            df_dpd.union(df_contract_termination)
            .union(df_debt_sale)
            .union(df_foreclosure)
            .union(df_cross_contagion)
            .union(df_cross_default)
            .union(df_credit_risk)
            .union(df_non_accrued)
            .union(df_breach_covenants)
        )

        # Write the new events to the database
        df_all_events.write.jdbc(
            url=postgres_properties["url"],
            table='"ABACUS_A5"."t19_default_events"',
            mode="append",
            properties=postgres_properties,
        )

        print("insert_default_events_cc completed successfully.")
    except Exception as e:
        print(f"Error in insert_default_events_cc: {str(e)}")


if __name__ == "__main__":
    insert_default_events_cc()
