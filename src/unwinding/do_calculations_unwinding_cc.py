from src.unwinding.calc_unwinding_ocy_cc import calc_unwinding_ocy_cc

from src.unwinding.calc_unwinding_mtd_cc import calc_unwinding_mtd_cc

from src.unwinding.calc_unwinding_daily_cc import calc_unwinding_daily_cc

from src.unwinding.load_prev_t14_cc import load_prev_t14_cc

from src.unwinding.insert_t14_unwinding_cc import insert_t14_unwinding_cc


def do_calculations_unwinding_cc():
    insert_t14_unwinding_cc()
    load_prev_t14_cc()
    calc_unwinding_daily_cc()
    calc_unwinding_mtd_cc()
    calc_unwinding_ocy_cc()


# Main execution
if __name__ == "__main__":
    do_calculations_unwinding_cc()
