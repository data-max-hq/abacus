from pyspark.sql.functions import col, lit, concat, when, max, add_months
from src.insert_default.calc_default_fields_cc import calc_default_fields_cc

from src.insert_default.calc_cross_default_cc import calc_cross_default_cc

from src.insert_default.calc_end_of_probation_cc import calc_end_of_probation_cc

from src.insert_default.calc_probation_cc import calc_probation_cc

from src.functions.no_dup_words import no_dup_words
from db_connection_util import (
    get_spark_session,
    get_postgres_properties,
    read_data_from_postgres,
    update_to_database,
)


def calculate_cc_default():
    # Get the Spark session and Postgres properties
    spark = get_spark_session()
    postgres_properties = get_postgres_properties()
    schema_name = "ABACUS_A5"

    # Read necessary tables
    df_w01 = read_data_from_postgres(spark, "w01_working_day", schema_name)
    df_w02 = read_data_from_postgres(spark, "w02_ccworking_day", schema_name)
    df_t17 = read_data_from_postgres(spark, "atmp_t17_dpd_credit_cards", schema_name)
    df_technical_default = read_data_from_postgres(
        spark, "technical_default", schema_name
    )
    df_tc08 = read_data_from_postgres(spark, "tc08_configuration_table", schema_name)
    df_default_triggers = read_data_from_postgres(
        spark, "atmp_default_triggers", schema_name
    )
    df_customer = read_data_from_postgres(spark, "atmp_customer", schema_name)
    df_t01 = read_data_from_postgres(spark, "t01_customer", schema_name)

    # Get working days
    i_workingday = df_w01.agg(max("working_day")).collect()[0][0]
    i_ccworkingday = df_w02.agg(max("working_day")).collect()[0][0]
    v_next_working_day = add_months(lit(i_ccworkingday), 1).cast("date")

    # Filter technical_default for the given working day
    df_technical_default_filtered = df_technical_default.filter(
        col("SYSTEM_DATE") == lit(i_workingday)
    )

    # First UPDATE operation (by id_product)
    df_t17_updated = (
        df_t17.alias("p")
        .join(
            df_technical_default_filtered.select(
                col("id_product").alias("t_id_product")
            )
            .distinct()
            .alias("t"),
            col("p.id_product") == col("t_id_product"),
            "left",
        )
        .withColumn(
            "p_is_probation",
            when(col("t_id_product").isNotNull(), lit("0")).otherwise(
                col("p_is_probation")
            ),
        )
        .withColumn(
            "p_proposed_default_ind",
            when(col("t_id_product").isNotNull(), lit("0")).otherwise(
                col("p_proposed_default_ind")
            ),
        )
        .withColumn(
            "p_start_probation_date",
            when(col("t_id_product").isNotNull(), lit(None)).otherwise(
                col("p_start_probation_date")
            ),
        )
        .withColumn(
            "p_end_probation_date",
            when(col("t_id_product").isNotNull(), lit(None)).otherwise(
                col("p_end_probation_date")
            ),
        )
        .drop("t_id_product")
    )

    # Removing aliases to avoid ambiguous references in the later joins
    df_t17_updated = df_t17_updated.select(
        *[col(c).alias(c) for c in df_t17_updated.columns]
    )

    # Second UPDATE operation (by customer_number)
    df_t17_updated = (
        df_t17_updated.alias("p")
        .join(
            df_technical_default_filtered.select(
                col("customer_number").alias("t_customer_number")
            )
            .distinct()
            .alias("t"),
            col("p.customer_number") == col("t_customer_number"),
            "left",
        )
        .withColumn(
            "p_is_probation",
            when(col("t_customer_number").isNotNull(), lit("0")).otherwise(
                col("p_is_probation")
            ),
        )
        .withColumn(
            "p_proposed_default_ind",
            when(col("t_customer_number").isNotNull(), lit("0")).otherwise(
                col("p_proposed_default_ind")
            ),
        )
        .withColumn(
            "p_start_probation_date",
            when(col("t_customer_number").isNotNull(), lit(None)).otherwise(
                col("p_start_probation_date")
            ),
        )
        .withColumn(
            "p_end_probation_date",
            when(col("t_customer_number").isNotNull(), lit(None)).otherwise(
                col("p_end_probation_date")
            ),
        )
        .drop("t_customer_number")
    )

    # Removing aliases to avoid ambiguous references in the later joins
    df_t17_updated = df_t17_updated.select(
        *[col(c).alias(c) for c in df_t17_updated.columns]
    )

    # initialization of default_indicator: added as a precondition for multiple runs on the same data
    df_t17_updated = (
        df_t17_updated.withColumn("default_indicator", lit("0"))
        .withColumn("default_reason", lit(""))
        .withColumn("proposed_default_ind", lit(0))
        .withColumn("is_probation", col("p_is_probation"))
        .withColumn("start_probation_date", col("p_start_probation_date"))
        .withColumn("end_probation_date", col("p_end_probation_date"))
    )

    # Default indicator for retail products meeting DPD conditions

    # First, get the DPD threshold for RETAIL segment
    dpd_threshold = (
        df_tc08.filter(col("segment_type") == "RETAIL")
        .select("dpd_threshold")
        .first()["dpd_threshold"]
    )

    # Now, perform the update operation
    df_t17_updated = (
        df_t17_updated.withColumn(
            "default_indicator",
            when(
                (col("dpd_ho") > lit(dpd_threshold))
                & (col("retail_nonretail_indicator") == "Retail"),
                lit(1),
            ).otherwise(col("default_indicator")),
        )
        .withColumn(
            "default_reason",
            when(
                (col("dpd_ho") > lit(dpd_threshold))
                & (col("retail_nonretail_indicator") == "Retail"),
                concat(lit(",DPD,"), col("p_default_reason")),
            ).otherwise(col("default_reason")),
        )
        .withColumn(
            "proposed_default_ind",
            when(
                (col("dpd_ho") > lit(dpd_threshold))
                & (col("retail_nonretail_indicator") == "Retail"),
                lit(1),
            ).otherwise(col("proposed_default_ind")),
        )
        .withColumn(
            "is_probation",
            when(
                (col("dpd_ho") > lit(dpd_threshold))
                & (col("retail_nonretail_indicator") == "Retail"),
                lit("0"),
            ).otherwise(col("is_probation")),
        )
        .withColumn(
            "start_probation_date",
            when(
                (col("dpd_ho") > lit(dpd_threshold))
                & (col("retail_nonretail_indicator") == "Retail"),
                lit(v_next_working_day),
            ).otherwise(col("start_probation_date")),
        )
        .withColumn(
            "end_probation_date",
            when(
                (col("dpd_ho") > lit(dpd_threshold))
                & (col("retail_nonretail_indicator") == "Retail"),
                add_months(lit(v_next_working_day), 3),
            ).otherwise(col("end_probation_date")),
        )
    )

    # DECEASED INDICATOR
    # First, filter the atmp_default_triggers DataFrame
    df_default_triggers_filtered = (
        df_default_triggers.filter(
            (col("working_day") == lit(i_workingday)) & (col("is_decease_cust") == "1")
        )
        .select(col("nrp_customer").alias("triggers_nrp_customer"))
        .distinct()
    )

    # Now, perform the MERGE operation
    df_t17_updated = (
        df_t17_updated.alias("t17")
        .join(
            df_default_triggers_filtered.alias("triggers"),
            (col("t17.customer_number") == col("triggers_nrp_customer"))
            & (col("t17.retail_nonretail_indicator") == "RETAIL")
            & (col("t17.is_joint") == "0"),
            "left",
        )
        .withColumn(
            "default_indicator",
            when(col("triggers_nrp_customer").isNotNull(), lit("1")).otherwise(
                col("default_indicator")
            ),
        )
        .withColumn(
            "default_reason",
            when(
                col("triggers_nrp_customer").isNotNull(),
                concat(col("default_reason"), lit(",DECEASED")),
            ).otherwise(col("default_reason")),
        )
        .withColumn(
            "proposed_default_ind",
            when(col("triggers_nrp_customer").isNotNull(), lit(1)).otherwise(
                col("proposed_default_ind")
            ),
        )
        .withColumn(
            "is_probation",
            when(col("triggers_nrp_customer").isNotNull(), lit(0)).otherwise(
                col("is_probation")
            ),
        )
        .withColumn(
            "start_probation_date",
            when(
                col("triggers_nrp_customer").isNotNull(), lit(v_next_working_day)
            ).otherwise(col("start_probation_date")),
        )
        .withColumn(
            "end_probation_date",
            when(
                col("triggers_nrp_customer").isNotNull(),
                add_months(lit(v_next_working_day), 3),
            ).otherwise(col("end_probation_date")),
        )
        .drop("triggers_nrp_customer")
    )

    # Removing aliases to avoid ambiguous references in the later joins
    df_t17_updated = df_t17_updated.select(
        *[col(c).alias(c) for c in df_t17_updated.columns]
    )

    # BANKCRUPTCY INDICATOR
    # First, filter the atmp_default_triggers DataFrame
    df_default_triggers_filtered = (
        df_default_triggers.filter(
            (col("working_day") == lit(i_workingday)) & (col("is_bankcruptcy") == "1")
        )
        .select(col("nrp_customer").alias("triggers_nrp_customer"))
        .distinct()
    )

    # Now, perform the MERGE operation
    df_t17_updated = (
        df_t17_updated.alias("t17")
        .join(
            df_default_triggers_filtered.alias("triggers"),
            (col("t17.customer_number") == col("triggers_nrp_customer"))
            & (col("t17.retail_nonretail_indicator") == "RETAIL")
            & (col("t17.is_joint") == "0"),
            "left",
        )
        .withColumn(
            "default_indicator",
            when(col("triggers_nrp_customer").isNotNull(), lit("1")).otherwise(
                col("default_indicator")
            ),
        )
        .withColumn(
            "default_reason",
            when(
                col("triggers_nrp_customer").isNotNull(),
                concat(col("default_reason"), lit(",BANKCRUPTCY")),
            ).otherwise(col("default_reason")),
        )
        .withColumn(
            "proposed_default_ind",
            when(col("triggers_nrp_customer").isNotNull(), lit(1)).otherwise(
                col("proposed_default_ind")
            ),
        )
        .withColumn(
            "is_probation",
            when(col("triggers_nrp_customer").isNotNull(), lit(0)).otherwise(
                col("is_probation")
            ),
        )
        .withColumn(
            "start_probation_date",
            when(
                col("triggers_nrp_customer").isNotNull(), lit(v_next_working_day)
            ).otherwise(col("start_probation_date")),
        )
        .withColumn(
            "end_probation_date",
            when(
                col("triggers_nrp_customer").isNotNull(),
                add_months(lit(v_next_working_day), 3),
            ).otherwise(col("end_probation_date")),
        )
        .drop("triggers_nrp_customer")
    )

    # Removing aliases to avoid ambiguous references in the later joins
    df_t17_updated = df_t17_updated.select(
        *[col(c).alias(c) for c in df_t17_updated.columns]
    )

    # TERMINATED INDICATOR
    # First, filter the atmp_default_triggers DataFrame
    df_default_triggers_filtered = (
        df_default_triggers.filter(
            (col("working_day") == lit(i_workingday))
            & (col("is_terminated_contract") == "1")
        )
        .select(col("id_product").alias("triggers_id_product"))
        .distinct()
    )

    # Now, perform the MERGE operation
    df_t17_updated = (
        df_t17_updated.alias("t17")
        .join(
            df_default_triggers_filtered.alias("triggers"),
            (col("t17.id_product") == col("triggers_id_product"))
            & (col("t17.retail_nonretail_indicator") == "RETAIL"),
            "left",
        )
        .withColumn(
            "default_indicator",
            when(col("triggers_id_product").isNotNull(), lit("1")).otherwise(
                col("default_indicator")
            ),
        )
        .withColumn(
            "default_reason",
            when(
                col("triggers_id_product").isNotNull(),
                concat(col("default_reason"), lit(",CONTRACT TERMINATION")),
            ).otherwise(col("default_reason")),
        )
        .withColumn(
            "proposed_default_ind",
            when(col("triggers_id_product").isNotNull(), lit(1)).otherwise(
                col("proposed_default_ind")
            ),
        )
        .withColumn(
            "is_probation",
            when(col("triggers_id_product").isNotNull(), lit(0)).otherwise(
                col("is_probation")
            ),
        )
        .withColumn(
            "start_probation_date",
            when(
                col("triggers_id_product").isNotNull(), lit(v_next_working_day)
            ).otherwise(col("start_probation_date")),
        )
        .withColumn(
            "end_probation_date",
            when(
                col("triggers_id_product").isNotNull(),
                add_months(lit(v_next_working_day), 3),
            ).otherwise(col("end_probation_date")),
        )
        .drop("triggers_id_product")
    )

    df_t17_updated = df_t17_updated.select(
        *[col(c).alias(c) for c in df_t17_updated.columns]
    )

    # LOSS OF SUFFICIENT INCOME INDICATOR
    # First, filter the atmp_default_triggers DataFrame
    df_default_triggers_filtered = (
        df_default_triggers.filter(col("loss_of_sufficient_income") == "1")
        .select(
            col("nrp_customer").alias("triggers_nrp_customer"),
            col("working_day").alias("triggers_working_day"),
        )
        .distinct()
    )

    # Filter the atmp_customer DataFrame for the EXISTS condition
    df_customer_filtered = (
        df_customer.filter(
            (col("retail_nonretail") == "RETAIL")
            & (col("has_product") == "1")
            & (col("max_dpd") > 60)
        )
        .select(col("nrp_customer").alias("customer_nrp_customer"))
        .distinct()
    )

    # Now, perform the MERGE operation
    df_t17_updated = (
        df_t17_updated.alias("t17")
        .join(
            df_default_triggers_filtered.alias("triggers"),
            (col("t17.customer_number") == col("triggers_nrp_customer"))
            & (col("t17.retail_nonretail_indicator") == "RETAIL")
            & (col("t17.is_joint") == "0")
            & (col("t17.working_day") <= add_months(col("triggers_working_day"), 3)),
            "left",
        )
        .join(
            df_customer_filtered.alias("customer"),
            col("t17.customer_number") == col("customer_nrp_customer"),
            "left",
        )
        .withColumn(
            "default_indicator",
            when(
                (col("triggers_nrp_customer").isNotNull())
                & (col("customer_nrp_customer").isNotNull()),
                lit("1"),
            ).otherwise(col("default_indicator")),
        )
        .withColumn(
            "default_reason",
            when(
                (col("triggers_nrp_customer").isNotNull())
                & (col("customer_nrp_customer").isNotNull()),
                concat(col("default_reason"), lit(",LOSS OF SUFFICIENT INCOME")),
            ).otherwise(col("default_reason")),
        )
        .withColumn(
            "proposed_default_ind",
            when(
                (col("triggers_nrp_customer").isNotNull())
                & (col("customer_nrp_customer").isNotNull()),
                lit(1),
            ).otherwise(col("proposed_default_ind")),
        )
        .withColumn(
            "is_probation",
            when(
                (col("triggers_nrp_customer").isNotNull())
                & (col("customer_nrp_customer").isNotNull()),
                lit(0),
            ).otherwise(col("is_probation")),
        )
        .withColumn(
            "start_probation_date",
            when(
                (col("triggers_nrp_customer").isNotNull())
                & (col("customer_nrp_customer").isNotNull()),
                lit(v_next_working_day),
            ).otherwise(col("start_probation_date")),
        )
        .withColumn(
            "end_probation_date",
            when(
                (col("triggers_nrp_customer").isNotNull())
                & (col("customer_nrp_customer").isNotNull()),
                add_months(lit(v_next_working_day), 3),
            ).otherwise(col("end_probation_date")),
        )
        .drop("triggers_nrp_customer", "triggers_working_day", "customer_nrp_customer")
    )

    df_t17_updated = df_t17_updated.select(
        *[col(c).alias(c) for c in df_t17_updated.columns]
    )
    # Similar operations for BANKRUPTCY, CONTRACT TERMINATION, LOSS OF SUFFICIENT INCOME, INDEBTEDNESS, NON ACCRUED STATUS, BREACH OF COVENANTS, DEBT SALE, FORECLOSURE COLLATERAL, and FRAUD

    # INDEBTEDNESS INDICATOR
    # First, filter the atmp_default_triggers DataFrame
    df_default_triggers_filtered = (
        df_default_triggers.filter(col("is_indebtedness") == "1")
        .select(
            col("nrp_customer").alias("triggers_nrp_customer"),
            col("working_day").alias("triggers_working_day"),
        )
        .distinct()
    )

    # Filter the atmp_customer DataFrame for the EXISTS condition
    df_customer_filtered = (
        df_customer.filter(
            (col("retail_nonretail") == "RETAIL")
            & (col("has_product") == "1")
            & (col("max_dpd") > 60)
        )
        .select(col("nrp_customer").alias("customer_nrp_customer"))
        .distinct()
    )

    # Now, perform the MERGE operation
    df_t17_updated = (
        df_t17_updated.alias("t17")
        .join(
            df_default_triggers_filtered.alias("triggers"),
            (col("t17.customer_number") == col("triggers_nrp_customer"))
            & (col("t17.retail_nonretail_indicator") == "RETAIL")
            & (col("t17.is_joint") == "0")
            & (col("t17.working_day") <= add_months(col("triggers_working_day"), 3)),
            "left",
        )
        .join(
            df_customer_filtered.alias("customer"),
            col("t17.customer_number") == col("customer_nrp_customer"),
            "left",
        )
        .withColumn(
            "default_indicator",
            when(
                (col("triggers_nrp_customer").isNotNull())
                & (col("customer_nrp_customer").isNotNull()),
                lit("1"),
            ).otherwise(col("default_indicator")),
        )
        .withColumn(
            "default_reason",
            when(
                (col("triggers_nrp_customer").isNotNull())
                & (col("customer_nrp_customer").isNotNull()),
                concat(col("default_reason"), lit(",INDEBTEDNESS")),
            ).otherwise(col("default_reason")),
        )
        .withColumn(
            "proposed_default_ind",
            when(
                (col("triggers_nrp_customer").isNotNull())
                & (col("customer_nrp_customer").isNotNull()),
                lit(1),
            ).otherwise(col("proposed_default_ind")),
        )
        .withColumn(
            "is_probation",
            when(
                (col("triggers_nrp_customer").isNotNull())
                & (col("customer_nrp_customer").isNotNull()),
                lit(0),
            ).otherwise(col("is_probation")),
        )
        .withColumn(
            "start_probation_date",
            when(
                (col("triggers_nrp_customer").isNotNull())
                & (col("customer_nrp_customer").isNotNull()),
                lit(v_next_working_day),
            ).otherwise(col("start_probation_date")),
        )
        .withColumn(
            "end_probation_date",
            when(
                (col("triggers_nrp_customer").isNotNull())
                & (col("customer_nrp_customer").isNotNull()),
                add_months(lit(v_next_working_day), 3),
            ).otherwise(col("end_probation_date")),
        )
        .drop("triggers_nrp_customer", "triggers_working_day", "customer_nrp_customer")
    )

    df_t17_updated = df_t17_updated.select(
        *[col(c).alias(c) for c in df_t17_updated.columns]
    )

    # NON ACCRUED STATUS INDICATOR
    # First, filter the atmp_default_triggers DataFrame
    df_default_triggers_filtered = (
        df_default_triggers.filter(
            (col("working_day") == lit(i_workingday))
            & (col("non_accrued_status") == "1")
        )
        .select(col("id_product").alias("triggers_id_product"))
        .distinct()
    )

    # Now, perform the MERGE operation
    df_t17_updated = (
        df_t17_updated.alias("t17")
        .join(
            df_default_triggers_filtered.alias("triggers"),
            (col("t17.id_product") == col("triggers_id_product"))
            & (col("t17.retail_nonretail_indicator") == "RETAIL")
            & (col("t17.is_joint") == "0"),
            "left",
        )
        .withColumn(
            "default_indicator",
            when(col("triggers_id_product").isNotNull(), lit("1")).otherwise(
                col("default_indicator")
            ),
        )
        .withColumn(
            "default_reason",
            when(
                col("triggers_id_product").isNotNull(),
                concat(col("default_reason"), lit(",NON ACCRUED STATUS")),
            ).otherwise(col("default_reason")),
        )
        .withColumn(
            "proposed_default_ind",
            when(col("triggers_id_product").isNotNull(), lit(1)).otherwise(
                col("proposed_default_ind")
            ),
        )
        .withColumn(
            "is_probation",
            when(col("triggers_id_product").isNotNull(), lit(0)).otherwise(
                col("is_probation")
            ),
        )
        .withColumn(
            "start_probation_date",
            when(
                col("triggers_id_product").isNotNull(), lit(v_next_working_day)
            ).otherwise(col("start_probation_date")),
        )
        .withColumn(
            "end_probation_date",
            when(
                col("triggers_id_product").isNotNull(),
                add_months(lit(v_next_working_day), 3),
            ).otherwise(col("end_probation_date")),
        )
        .drop("triggers_id_product")
    )

    df_t17_updated = df_t17_updated.select(
        *[col(c).alias(c) for c in df_t17_updated.columns]
    )

    # BREACH OF COVENANTS INDICATOR
    # First, filter the atmp_default_triggers DataFrame
    df_default_triggers_filtered = (
        df_default_triggers.filter(col("breach_contract_covenants") == "1")
        .select(
            col("id_product").alias("triggers_id_product"),
            col("working_day").alias("triggers_working_day"),
        )
        .distinct()
    )

    # Now, perform the MERGE operation
    df_t17_updated = (
        df_t17_updated.alias("nrm")
        .join(
            df_default_triggers_filtered.alias("src"),
            (col("nrm.id_product") == col("triggers_id_product"))
            & (col("nrm.working_day") == col("triggers_working_day"))
            & (col("nrm.retail_nonretail_indicator") == "RETAIL"),
            "left",
        )
        .withColumn(
            "default_indicator",
            when(col("triggers_id_product").isNotNull(), lit("1")).otherwise(
                col("default_indicator")
            ),
        )
        .withColumn(
            "default_reason",
            when(
                col("triggers_id_product").isNotNull(),
                concat(col("default_reason"), lit(",BREACH OF COVENANTS")),
            ).otherwise(col("default_reason")),
        )
        .withColumn(
            "proposed_default_ind",
            when(col("triggers_id_product").isNotNull(), lit(1)).otherwise(
                col("proposed_default_ind")
            ),
        )
        .withColumn(
            "is_probation",
            when(col("triggers_id_product").isNotNull(), lit(0)).otherwise(
                col("is_probation")
            ),
        )
        .withColumn(
            "start_probation_date",
            when(
                col("triggers_id_product").isNotNull(), lit(v_next_working_day)
            ).otherwise(col("start_probation_date")),
        )
        .withColumn(
            "end_probation_date",
            when(
                col("triggers_id_product").isNotNull(),
                add_months(lit(v_next_working_day), 3),
            ).otherwise(col("end_probation_date")),
        )
        .drop("triggers_id_product", "triggers_working_day")
    )

    df_t17_updated = df_t17_updated.select(
        *[col(c).alias(c) for c in df_t17_updated.columns]
    )

    # DEBT SALE INDICATOR
    # First, filter the atmp_default_triggers DataFrame
    df_default_triggers_filtered = (
        df_default_triggers.filter(
            (col("working_day") == lit(i_workingday)) & (col("is_dept_sale_npv") == "1")
        )
        .select(col("id_product").alias("triggers_id_product"))
        .distinct()
    )

    # Now, perform the MERGE operation
    df_t17_updated = (
        df_t17_updated.alias("t17")
        .join(
            df_default_triggers_filtered.alias("triggers"),
            (col("t17.id_product") == col("triggers_id_product"))
            & (col("t17.retail_nonretail_indicator") == "RETAIL"),
            "left",
        )
        .withColumn(
            "default_indicator",
            when(col("triggers_id_product").isNotNull(), lit("1")).otherwise(
                col("default_indicator")
            ),
        )
        .withColumn(
            "default_reason",
            when(
                col("triggers_id_product").isNotNull(),
                concat(col("default_reason"), lit(",DEBT SALE")),
            ).otherwise(col("default_reason")),
        )
        .withColumn(
            "proposed_default_ind",
            when(col("triggers_id_product").isNotNull(), lit(1)).otherwise(
                col("proposed_default_ind")
            ),
        )
        .withColumn(
            "is_probation",
            when(col("triggers_id_product").isNotNull(), lit(0)).otherwise(
                col("is_probation")
            ),
        )
        .withColumn(
            "start_probation_date",
            when(
                col("triggers_id_product").isNotNull(), lit(v_next_working_day)
            ).otherwise(col("start_probation_date")),
        )
        .withColumn(
            "end_probation_date",
            when(
                col("triggers_id_product").isNotNull(),
                add_months(lit(v_next_working_day), 3),
            ).otherwise(col("end_probation_date")),
        )
        .drop("triggers_id_product")
    )

    df_t17_updated = df_t17_updated.select(
        *[col(c).alias(c) for c in df_t17_updated.columns]
    )

    # FORECLOSURE COLLATERAL INDICATOR
    # First, filter the atmp_default_triggers DataFrame
    df_default_triggers_filtered = (
        df_default_triggers.filter(
            (col("working_day") == lit(i_workingday))
            & (col("is_foreclosure_collateral") == "1")
        )
        .select(col("id_product").alias("triggers_id_product"))
        .distinct()
    )

    # Now, perform the MERGE operation
    df_t17_updated = (
        df_t17_updated.alias("t17")
        .join(
            df_default_triggers_filtered.alias("triggers"),
            (col("t17.id_product") == col("triggers_id_product"))
            & (col("t17.retail_nonretail_indicator") == "RETAIL"),
            "left",
        )
        .withColumn(
            "default_indicator",
            when(col("triggers_id_product").isNotNull(), lit("1")).otherwise(
                col("default_indicator")
            ),
        )
        .withColumn(
            "default_reason",
            when(
                col("triggers_id_product").isNotNull(),
                concat(col("default_reason"), lit(",FORECLOSURE COLLATERAL")),
            ).otherwise(col("default_reason")),
        )
        .withColumn(
            "proposed_default_ind",
            when(col("triggers_id_product").isNotNull(), lit(1)).otherwise(
                col("proposed_default_ind")
            ),
        )
        .withColumn(
            "is_probation",
            when(col("triggers_id_product").isNotNull(), lit(0)).otherwise(
                col("is_probation")
            ),
        )
        .withColumn(
            "start_probation_date",
            when(
                col("triggers_id_product").isNotNull(), lit(v_next_working_day)
            ).otherwise(col("start_probation_date")),
        )
        .withColumn(
            "end_probation_date",
            when(
                col("triggers_id_product").isNotNull(),
                add_months(lit(v_next_working_day), 3),
            ).otherwise(col("end_probation_date")),
        )
        .drop("triggers_id_product")
    )

    df_t17_updated = df_t17_updated.select(
        *[col(c).alias(c) for c in df_t17_updated.columns]
    )

    # FRAUD INDICATOR
    # First, filter the atmp_default_triggers DataFrame
    df_default_triggers_filtered = (
        df_default_triggers.filter(
            (col("working_day") == lit(i_workingday))
            & (col("is_foreclosure_collateral") == "1")
        )
        .select(col("id_product").alias("triggers_id_product"))
        .distinct()
    )

    # Now, perform the MERGE operation
    df_t17_updated = (
        df_t17_updated.alias("t17")
        .join(
            df_default_triggers_filtered.alias("triggers"),
            (col("t17.id_product") == col("triggers_id_product"))
            & (col("t17.retail_nonretail_indicator") == "RETAIL"),
            "left",
        )
        .withColumn(
            "default_indicator",
            when(col("triggers_id_product").isNotNull(), lit("1")).otherwise(
                col("default_indicator")
            ),
        )
        .withColumn(
            "default_reason",
            when(
                col("triggers_id_product").isNotNull(),
                concat(col("default_reason"), lit(",FORECLOSURE COLLATERAL")),
            ).otherwise(col("default_reason")),
        )
        .withColumn(
            "proposed_default_ind",
            when(col("triggers_id_product").isNotNull(), lit(1)).otherwise(
                col("proposed_default_ind")
            ),
        )
        .withColumn(
            "is_probation",
            when(col("triggers_id_product").isNotNull(), lit(0)).otherwise(
                col("is_probation")
            ),
        )
        .withColumn(
            "start_probation_date",
            when(
                col("triggers_id_product").isNotNull(), lit(v_next_working_day)
            ).otherwise(col("start_probation_date")),
        )
        .withColumn(
            "end_probation_date",
            when(
                col("triggers_id_product").isNotNull(),
                add_months(lit(v_next_working_day), 3),
            ).otherwise(col("end_probation_date")),
        )
        .drop("triggers_id_product")
    )

    df_t17_updated = df_t17_updated.select(
        *[col(c).alias(c) for c in df_t17_updated.columns]
    )

    # Nonretail default_indicator
    # Perform the MERGE operation
    df_t17_updated = (
        df_t17_updated.alias("t17")
        .join(
            df_t01.select(
                col("nrp_customer").alias("t01_nrp_customer"),
                col("default_indicator").alias("t01_default_indicator"),
            ).alias("t01"),
            (col("t17.customer_number") == col("t01_nrp_customer"))
            & (col("t17.retail_nonretail_indicator") == "NONRETAIL"),
            "left",
        )
        .withColumn(
            "default_indicator",
            when(
                col("t01_nrp_customer").isNotNull(), col("t01_default_indicator")
            ).otherwise(col("default_indicator")),
        )
        .withColumn(
            "default_reason",
            when(
                (col("t01_nrp_customer").isNotNull())
                & (col("t01_default_indicator") == "1"),
                lit(",DPD"),
            ).otherwise(lit("")),
        )
        .drop("t01_nrp_customer", "t01_default_indicator")
    )

    df_t17_updated = df_t17_updated.select(
        *[col(c).alias(c) for c in df_t17_updated.columns]
    )

    # Apply the PySpark function with the conditional logic to remove duplicate words
    df_t17_updated = no_dup_words(df_t17_updated)

    df_updated = df_t17_updated.select(
        "id_product",
        "default_indicator",
        "default_reason",
        "proposed_default_ind",
        "is_probation",
        "start_probation_date",
        "end_probation_date",
        "p_is_probation",
        "p_proposed_default_ind",
        "p_start_probation_date",
        "p_end_probation_date",
    )

    # Write the final dataframe to a temporary table
    df_updated.write.jdbc(
        url=postgres_properties["url"],
        table='"ABACUS_A5"."temp_updated_rows"',
        mode="overwrite",  # Use overwrite for temporary table
        properties=postgres_properties,
    )

    update_query = """
            UPDATE "ABACUS_A5"."atmp_t17_dpd_credit_cards" AS t17
            SET
                default_indicator = temp.default_indicator,
                default_reason = temp.default_reason,
                proposed_default_ind = temp.proposed_default_ind,
                is_probation = temp.is_probation,
                start_probation_date = temp.start_probation_date,
                end_probation_date = temp.end_probation_date,
                p_is_probation = temp.p_is_probation,
                p_proposed_default_ind = temp.p_proposed_default_ind,
                p_start_probation_date = temp.p_start_probation_date,
                p_end_probation_date = temp.p_end_probation_date
            FROM "ABACUS_A5"."temp_updated_rows" AS temp
            WHERE t17.id_product = temp.id_product;
            """

    update_to_database(update_query)

    # Call additional procedures
    calc_probation_cc()
    calc_end_of_probation_cc()
    calc_cross_default_cc()
    calc_default_fields_cc()

    print("calculate_cc_default completed successfully.")


if __name__ == "__main__":
    calculate_cc_default()
