import sys
from datetime import datetime, timedelta

# ============================================================
# ADD DATA WAREHOUSE PROJECT TO PYTHON PATH
# ============================================================

sys.path.insert(
    0,
    "/opt/airflow/data_warehouse"
)

import pandas as pd

from airflow.sdk import DAG, task


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="warehouse_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=[
        "data_warehouse",
        "etl",
        "incremental"
    ]
) as dag:

    # ========================================================
    # TEST WAREHOUSE CONNECTION
    # ========================================================

    @task(
        retries=2,
        retry_delay=timedelta(seconds=30)
    )
    def test_warehouse_connection():

        from airflow.providers.postgres.hooks.postgres import (
            PostgresHook
        )

        hook = PostgresHook(
            postgres_conn_id="warehouse_postgres"
        )

        connection = hook.get_conn()

        try:

            cursor = connection.cursor()

            cursor.execute("SELECT 1")

            result = cursor.fetchone()[0]

            cursor.close()

            if result != 1:

                raise ValueError(
                    "Warehouse PostgreSQL "
                    "connection test failed"
                )

            print(
                "Warehouse PostgreSQL "
                "connection successful"
            )

        finally:

            connection.close()


    # ========================================================
    # INITIALIZE PIPELINE METADATA
    # ========================================================

    @task
    def initialize_pipeline_metadata():

        from load import (
            create_pipeline_metadata_table
        )

        create_pipeline_metadata_table()

        print(
            "Pipeline metadata initialized "
            "successfully"
        )


    # ========================================================
    # EXTRACT
    # ========================================================

    @task(
        retries=2,
        retry_delay=timedelta(seconds=30)
    )
    def extract_data():

        from extract import (
            extract_customers,
            extract_products,
            extract_orders
        )

        from load import (
            get_pipeline_watermark
        )

        import os

        staging_dir = (
            "/opt/airflow/data_warehouse/staging"
        )

        os.makedirs(
            staging_dir,
            exist_ok=True
        )

        # ----------------------------------------------------
        # Extract source data
        # ----------------------------------------------------

        customers = extract_customers()

        products = extract_products()

        orders = extract_orders()

        # ----------------------------------------------------
        # Get watermark from PostgreSQL
        # ----------------------------------------------------

        watermark = get_pipeline_watermark(
            "warehouse_pipeline"
        )

        watermark_date = pd.to_datetime(
            watermark
        )

        print(
            f"Current watermark: "
            f"{watermark_date}"
        )

        # ----------------------------------------------------
        # One-day lookback
        # ----------------------------------------------------

        lookback_days = 1

        lookback_date = (
            watermark_date
            - pd.Timedelta(
                days=lookback_days
            )
        )

        print(
            f"Lookback date: "
            f"{lookback_date}"
        )

        # ----------------------------------------------------
        # Convert order date
        # ----------------------------------------------------

        orders["order_date"] = pd.to_datetime(
            orders["order_date"]
        )

        # ----------------------------------------------------
        # Incremental filtering
        # ----------------------------------------------------

        incremental_orders = orders[
            orders["order_date"] > lookback_date
        ].copy()

        print(
            f"Total source orders: "
            f"{len(orders)}"
        )

        print(
            f"Orders selected for processing: "
            f"{len(incremental_orders)}"
        )

        # ----------------------------------------------------
        # Write staging files
        # ----------------------------------------------------

        customers.to_csv(
            f"{staging_dir}/customers.csv",
            index=False
        )

        products.to_csv(
            f"{staging_dir}/products.csv",
            index=False
        )

        incremental_orders.to_csv(
            f"{staging_dir}/orders.csv",
            index=False
        )

        # ----------------------------------------------------
        # Determine new watermark
        # ----------------------------------------------------

        if not incremental_orders.empty:

            latest_order_date = (
                incremental_orders["order_date"]
                .max()
                .strftime("%Y-%m-%d")
            )

        else:

            latest_order_date = watermark

        # ----------------------------------------------------
        # Store watermark candidate
        # ----------------------------------------------------

        with open(
            f"{staging_dir}/watermark.txt",
            "w"
        ) as f:

            f.write(
                latest_order_date
            )

        print(
            f"New watermark candidate: "
            f"{latest_order_date}"
        )


    # ========================================================
    # TRANSFORM
    # ========================================================

    @task(
        retries=1,
        retry_delay=timedelta(seconds=30)
    )
    def transform_data():

        from transform import (
            transform_customers,
            transform_products,
            transform_orders,
            add_product_keys,
            create_date_dimension
        )

        staging_dir = (
            "/opt/airflow/data_warehouse/staging"
        )

        # ----------------------------------------------------
        # Read staged source data
        # ----------------------------------------------------

        customers = pd.read_csv(
            f"{staging_dir}/customers.csv"
        )

        products = pd.read_csv(
            f"{staging_dir}/products.csv"
        )

        orders = pd.read_csv(
            f"{staging_dir}/orders.csv"
        )

        # ----------------------------------------------------
        # Transform customers
        # ----------------------------------------------------

        customers = transform_customers(
            customers
        )

        # ----------------------------------------------------
        # Transform products
        # ----------------------------------------------------

        products = transform_products(
            products
        )

        # ----------------------------------------------------
        # Transform orders
        # ----------------------------------------------------

        orders = transform_orders(
            orders
        )

        # ----------------------------------------------------
        # Add product keys
        #
        # IMPORTANT:
        # Existing transform.py expects ONLY products
        # ----------------------------------------------------

        products = add_product_keys(
            products
        )

        # ----------------------------------------------------
        # Create date dimension
        # ----------------------------------------------------

        dates = create_date_dimension(
            orders
        )

        # ----------------------------------------------------
        # Save transformed data
        # ----------------------------------------------------

        customers.to_csv(
            f"{staging_dir}/customers_transformed.csv",
            index=False
        )

        products.to_csv(
            f"{staging_dir}/products_transformed.csv",
            index=False
        )

        orders.to_csv(
            f"{staging_dir}/orders_transformed.csv",
            index=False
        )

        dates.to_csv(
            f"{staging_dir}/dates_transformed.csv",
            index=False
        )

        print(
            "Transformation completed "
            "successfully!"
        )


    # ========================================================
    # LOAD DIMENSIONS + FACT
    # ========================================================

    @task(
        retries=2,
        retry_delay=timedelta(seconds=30)
    )
    def load_dimensions_and_fact():

        from load import (
            create_warehouse_tables,
            process_customer_scd2,
            get_current_customer_keys,
            load_products,
            load_dates,
            load_fact_sales
        )

        from transform import (
            create_fact_sales
        )

        staging_dir = (
            "/opt/airflow/data_warehouse/staging"
        )

        # ----------------------------------------------------
        # Read transformed data
        # ----------------------------------------------------

        customers = pd.read_csv(
            f"{staging_dir}/customers_transformed.csv"
        )

        products = pd.read_csv(
            f"{staging_dir}/products_transformed.csv"
        )

        orders = pd.read_csv(
            f"{staging_dir}/orders_transformed.csv"
        )

        dates = pd.read_csv(
            f"{staging_dir}/dates_transformed.csv"
        )

        # ----------------------------------------------------
        # Restore datetime types
        # ----------------------------------------------------

        orders["order_date"] = pd.to_datetime(
            orders["order_date"]
        )

        dates["full_date"] = pd.to_datetime(
            dates["full_date"]
        )

        # ----------------------------------------------------
        # Create warehouse tables
        # ----------------------------------------------------

        create_warehouse_tables()

        # ----------------------------------------------------
        # Customer SCD Type 2
        # ----------------------------------------------------

        process_customer_scd2(
            customers
        )

        # ----------------------------------------------------
        # Get current customer surrogate keys
        # ----------------------------------------------------

        customer_keys = (
            get_current_customer_keys()
        )

        customer_key_df = pd.DataFrame(
            customer_keys,
            columns=[
                "customer_id",
                "customer_key"
            ]
        )

        # ----------------------------------------------------
        # Create fact table data
        #
        # Existing transform.py expects:
        # orders
        # customer_key_df
        # products
        # ----------------------------------------------------

        fact = create_fact_sales(
            orders,
            customer_key_df,
            products
        )

        # ----------------------------------------------------
        # Load product dimension
        # ----------------------------------------------------

        load_products(
            products
        )

        # ----------------------------------------------------
        # Load date dimension
        # ----------------------------------------------------

        load_dates(
            dates
        )

        # ----------------------------------------------------
        # Load fact table
        # Transaction-safe + UPSERT
        # ----------------------------------------------------

        load_fact_sales(
            fact
        )

        print(
            "Dimensions and fact loaded "
            "successfully!"
        )


    # ========================================================
    # ADVANCED DATA QUALITY CHECK
    # ========================================================

    @task
    def data_quality_check():

        from airflow.providers.postgres.hooks.postgres import (
            PostgresHook
        )

        hook = PostgresHook(
            postgres_conn_id="warehouse_postgres"
        )

        connection = hook.get_conn()

        try:

            cursor = connection.cursor()

            print(
                "===================================="
            )

            print(
                "Starting data quality checks..."
            )

            print(
                "===================================="
            )

            # ------------------------------------------------
            # 1. Table not empty checks
            # ------------------------------------------------

            tables = [
                "dim_customer",
                "dim_product",
                "dim_date",
                "fact_sales"
            ]

            for table in tables:

                cursor.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {table}
                    """
                )

                count = cursor.fetchone()[0]

                print(
                    f"{table}: {count} rows"
                )

                if count == 0:

                    raise ValueError(
                        f"DQ FAILED: "
                        f"{table} is empty"
                    )

            print(
                "✓ Table count checks passed"
            )

            # ------------------------------------------------
            # 2. Duplicate order IDs
            # ------------------------------------------------

            cursor.execute("""
                SELECT
                    order_id,
                    COUNT(*)
                FROM fact_sales
                GROUP BY order_id
                HAVING COUNT(*) > 1
            """)

            duplicates = cursor.fetchall()

            if duplicates:

                raise ValueError(
                    "DQ FAILED: duplicate "
                    f"order IDs found: "
                    f"{duplicates}"
                )

            print(
                "✓ Duplicate order check passed"
            )

            # ------------------------------------------------
            # 3. NULL checks
            # ------------------------------------------------

            cursor.execute("""
                SELECT COUNT(*)
                FROM fact_sales
                WHERE
                    order_id IS NULL
                    OR customer_key IS NULL
                    OR product_key IS NULL
                    OR date_key IS NULL
                    OR quantity IS NULL
                    OR sales_amount IS NULL
            """)

            null_count = (
                cursor.fetchone()[0]
            )

            if null_count > 0:

                raise ValueError(
                    f"DQ FAILED: "
                    f"{null_count} fact rows "
                    f"contain NULL values"
                )

            print(
                "✓ NULL check passed"
            )

            # ------------------------------------------------
            # 4. Quantity validation
            # ------------------------------------------------

            cursor.execute("""
                SELECT COUNT(*)
                FROM fact_sales
                WHERE quantity <= 0
            """)

            invalid_quantity = (
                cursor.fetchone()[0]
            )

            if invalid_quantity > 0:

                raise ValueError(
                    "DQ FAILED: "
                    f"{invalid_quantity} rows "
                    "have quantity <= 0"
                )

            print(
                "✓ Quantity check passed"
            )

            # ------------------------------------------------
            # 5. Sales amount validation
            # ------------------------------------------------

            cursor.execute("""
                SELECT COUNT(*)
                FROM fact_sales
                WHERE sales_amount < 0
            """)

            negative_sales = (
                cursor.fetchone()[0]
            )

            if negative_sales > 0:

                raise ValueError(
                    "DQ FAILED: "
                    f"{negative_sales} rows "
                    "have negative sales amount"
                )

            print(
                "✓ Sales amount check passed"
            )

            # ------------------------------------------------
            # 6. Customer foreign key validation
            # ------------------------------------------------

            cursor.execute("""
                SELECT COUNT(*)
                FROM fact_sales f
                LEFT JOIN dim_customer c
                    ON f.customer_key =
                       c.customer_key
                WHERE c.customer_key IS NULL
            """)

            invalid_customers = (
                cursor.fetchone()[0]
            )

            if invalid_customers > 0:

                raise ValueError(
                    "DQ FAILED: "
                    f"{invalid_customers} invalid "
                    "customer foreign keys"
                )

            print(
                "✓ Customer foreign key "
                "check passed"
            )

            # ------------------------------------------------
            # 7. Product foreign key validation
            # ------------------------------------------------

            cursor.execute("""
                SELECT COUNT(*)
                FROM fact_sales f
                LEFT JOIN dim_product p
                    ON f.product_key =
                       p.product_key
                WHERE p.product_key IS NULL
            """)

            invalid_products = (
                cursor.fetchone()[0]
            )

            if invalid_products > 0:

                raise ValueError(
                    "DQ FAILED: "
                    f"{invalid_products} invalid "
                    "product foreign keys"
                )

            print(
                "✓ Product foreign key "
                "check passed"
            )

            # ------------------------------------------------
            # 8. Date foreign key validation
            # ------------------------------------------------

            cursor.execute("""
                SELECT COUNT(*)
                FROM fact_sales f
                LEFT JOIN dim_date d
                    ON f.date_key =
                       d.date_key
                WHERE d.date_key IS NULL
            """)

            invalid_dates = (
                cursor.fetchone()[0]
            )

            if invalid_dates > 0:

                raise ValueError(
                    "DQ FAILED: "
                    f"{invalid_dates} invalid "
                    "date foreign keys"
                )

            print(
                "✓ Date foreign key "
                "check passed"
            )

            # ------------------------------------------------
            # SUCCESS
            # ------------------------------------------------

            print(
                "===================================="
            )

            print(
                "ALL DATA QUALITY CHECKS PASSED!"
            )

            print(
                "===================================="
            )

            cursor.close()

        finally:

            connection.close()


    # ========================================================
    # UPDATE WATERMARK
    # ========================================================

    @task
    def update_watermark():

        from load import (
            set_pipeline_watermark
        )

        staging_dir = (
            "/opt/airflow/data_warehouse/staging"
        )

        # ----------------------------------------------------
        # Read watermark generated by extract task
        # ----------------------------------------------------

        with open(
            f"{staging_dir}/watermark.txt",
            "r"
        ) as f:

            watermark = f.read().strip()

        # ----------------------------------------------------
        # Update PostgreSQL metadata table
        # ----------------------------------------------------

        set_pipeline_watermark(
            "warehouse_pipeline",
            watermark
        )

        print(
            f"Watermark updated successfully: "
            f"{watermark}"
        )


    # ========================================================
    # TASK INSTANCES
    # ========================================================

    connection_test = (
        test_warehouse_connection()
    )

    metadata = (
        initialize_pipeline_metadata()
    )

    extract = (
        extract_data()
    )

    transform = (
        transform_data()
    )

    load = (
        load_dimensions_and_fact()
    )

    dq = (
        data_quality_check()
    )

    watermark = (
        update_watermark()
    )

    # ========================================================
    # DEPENDENCIES
    # ========================================================

    connection_test >> metadata >> extract

    extract >> transform >> load >> dq >> watermark