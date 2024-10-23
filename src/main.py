# main.py
from src.abacus_insert_atmp_t17.calculate_atmp_cc_formulas import (
    calculate_atmp_cc_formulas,
)
from src.abacus_insert_atmp_t17.check_customer_product import check_customer_product
from src.abacus_insert_atmp_t17.check_product_type import check_product_type
from src.abacus_insert_atmp_t17.commit_atmp17_into_t17 import commit_atmpt17_into_t17
from src.abacus_insert_atmp_t17.commit_atmp18_into_t18 import commit_atmpt18_into_t18
from src.abacus_insert_atmp_t17.load_prev_atmp_t17 import load_prev_atmp_t17


def main():
    print(
        "================ Starting Credit Cards PySpark script orchestration ================"
    )

    print("Calling load_prev_atmp_t17")
    load_prev_atmp_t17()

    print("Calling calculate_atmp_cc_formulas")
    calculate_atmp_cc_formulas()

    print("Calling check_product_type")
    check_product_type()

    print("Calling check_customer_product")
    check_customer_product()

    print("Calling commit_atmpt17_into_t17")
    commit_atmpt17_into_t17()

    print("Calling commit_atmpt18_into_t18")
    commit_atmpt18_into_t18()

    print(
        "================ Credit Cards PySpark script orchestration completed ================"
    )


if __name__ == "__main__":
    main()
