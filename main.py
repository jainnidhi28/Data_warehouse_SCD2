import logging

import pandas as pd

from extract import (
    extract_customers,
    extract_products,
    extract_orders
)

from transform import (
    transform_customers,
    transform_products,
    transform_orders,
    add_product_keys,
    create_date_dimension,
    create_fact_sales
)

from load import (
    create_warehouse_tables,
    process_customer_scd2,
    get_current_customer_keys,
    load_products,
    load_dates,
    load_fact_sales
)


# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s - "
        "%(levelname)s - "
        "%(message)s"
    )
)


# =========================================================
# MAIN PIPELINE
# =========================================================

def main():

    try:

        logging.info(
            "Starting data warehouse pipeline"
        )

        # =================================================
        # EXTRACT
        # =================================================

        customers = extract_customers()

        products = extract_products()

        orders = extract_orders()

        logging.info(
            f"Extracted "
            f"{len(customers)} customers, "
            f"{len(products)} products, "
            f"{len(orders)} orders"
        )

        # =================================================
        # TRANSFORM
        # =================================================

        customers = transform_customers(
            customers
        )

        products = transform_products(
            products
        )

        orders = transform_orders(
            orders
        )

        products = add_product_keys(
            products
        )

        logging.info(
            "Data transformation completed"
        )

        # =================================================
        # CREATE TABLES
        # =================================================

        create_warehouse_tables()

        # =================================================
        # SCD TYPE 2
        # =================================================

        process_customer_scd2(
            customers
        )

        logging.info(
            "Customer SCD Type 2 processing completed"
        )

        # =================================================
        # GET CURRENT CUSTOMER KEYS
        # =================================================

        current_customer_keys = (
            get_current_customer_keys()
        )

        customer_keys_df = pd.DataFrame(
            current_customer_keys,
            columns=[
                "customer_id",
                "customer_key"
            ]
        )

        # =================================================
        # DATE DIMENSION
        # =================================================

        dates = create_date_dimension(
            orders
        )

        # =================================================
        # FACT TABLE
        # =================================================

        fact_sales = create_fact_sales(
            orders,
            customer_keys_df,
            products
        )

        logging.info(
            f"Prepared "
            f"{len(fact_sales)} fact records"
        )

        # =================================================
        # LOAD
        # =================================================

        load_products(
            products
        )

        load_dates(
            dates
        )

        load_fact_sales(
            fact_sales
        )

        # =================================================
        # COMPLETE
        # =================================================

        logging.info(
            "Data warehouse pipeline completed successfully"
        )

    except Exception as e:

        logging.error(
            f"Pipeline failed: {e}"
        )

        raise


if __name__ == "__main__":

    main()