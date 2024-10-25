from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from db_connection_util import get_spark_session

from src.insert_atmp_t17.calculate_atmp_cc_formulas import calculate_atmp_cc_formulas
from src.insert_atmp_t17.check_customer_product import check_customer_product
from src.insert_atmp_t17.check_product_type import check_product_type
from src.insert_atmp_t17.commit_atmp17_into_t17 import commit_atmpt17_into_t17
from src.insert_atmp_t17.commit_atmp18_into_t18 import commit_atmpt18_into_t18
from src.insert_atmp_t17.load_prev_atmp_t17 import load_prev_atmp_t17


def main():
    print(
        "================ Starting Credit Cards PySpark script orchestration ================"
    )

    # print("Calling load_prev_atmp_t17")
    # load_prev_atmp_t17()
    #
    # print("Calling calculate_atmp_cc_formulas")
    # calculate_atmp_cc_formulas()
    #
    # print("Calling check_product_type")
    # check_product_type()
    #
    # print("Calling check_customer_product")
    # check_customer_product()
    #
    # print("Calling commit_atmpt17_into_t17")
    # commit_atmpt17_into_t17()
    #
    # print("Calling commit_atmpt18_into_t18")
    # commit_atmpt18_into_t18()

    print(
        "================ Credit Cards PySpark script orchestration completed ================"
    )


def dummy():
    spark = SparkSession.builder \
        .appName("SimplePySparkExample") \
        .config("spark.driver.host", "localhost") \
        .config("spark.driver.port", "0") \
        .getOrCreate()

    # Create dummy data
    data = [
        ("Alice", 28, "Sales", 50000),
        ("Bob", 35, "Engineering", 75000),
        ("Charlie", 42, "Marketing", 60000),
        ("David", 31, "Sales", 55000),
        ("Eva", 39, "Engineering", 80000),
        ("Frank", 45, "Marketing", 65000),
        ("Grace", 33, "Sales", 52000),
        ("Henry", 37, "Engineering", 78000)
    ]

    # Define the schema
    columns = ["name", "age", "department", "salary"]

    # Create a DataFrame
    df = spark.createDataFrame(data, columns)

    # Show the DataFrame
    print("Original DataFrame:")
    df.show()

    # Perform some operations
    # 1. Filter employees older than 35
    older_employees = df.filter(col("age") > 35)


if __name__ == "__main__":
    dummy()
