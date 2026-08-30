from datetime import date

import psycopg

from config import (
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PASSWORD
)


def get_connection():

    return psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


# =========================================================
# CREATE WAREHOUSE TABLES
# =========================================================

def create_warehouse_tables():

    connection = get_connection()
    cursor = connection.cursor()

    # -------------------------
    # Customer Dimension
    # -------------------------

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_customer (

            customer_key INTEGER PRIMARY KEY,

            customer_id INTEGER NOT NULL,

            customer_name VARCHAR(100),

            city VARCHAR(100),

            age INTEGER,

            start_date DATE NOT NULL,

            end_date DATE,

            is_current BOOLEAN NOT NULL

        )
        """
    )

    # -------------------------
    # Product Dimension
    # -------------------------

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_product (

            product_key INTEGER PRIMARY KEY,

            product_id INTEGER UNIQUE NOT NULL,

            product_name VARCHAR(100),

            category VARCHAR(100),

            price NUMERIC(12, 2)

        )
        """
    )

    # -------------------------
    # Date Dimension
    # -------------------------

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_date (

            date_key INTEGER PRIMARY KEY,

            full_date DATE UNIQUE NOT NULL,

            day INTEGER,

            month INTEGER,

            quarter INTEGER,

            year INTEGER

        )
        """
    )

    # -------------------------
    # Fact Sales
    # -------------------------

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_sales (

            sales_key SERIAL PRIMARY KEY,

            order_id INTEGER UNIQUE NOT NULL,

            customer_key INTEGER NOT NULL,

            product_key INTEGER NOT NULL,

            date_key INTEGER NOT NULL,

            quantity INTEGER,

            sales_amount NUMERIC(12, 2),

            FOREIGN KEY (customer_key)
                REFERENCES dim_customer(customer_key),

            FOREIGN KEY (product_key)
                REFERENCES dim_product(product_key),

            FOREIGN KEY (date_key)
                REFERENCES dim_date(date_key)

        )
        """
    )

    connection.commit()

    cursor.close()
    connection.close()

    print(
        "Data warehouse tables created successfully!"
    )


# =========================================================
# SCD TYPE 2 - CUSTOMER
# =========================================================

def process_customer_scd2(df):

    connection = get_connection()
    cursor = connection.cursor()

    effective_date = date.today()

    for _, row in df.iterrows():

        customer_id = int(
            row["customer_id"]
        )

        customer_name = row["name"]

        city = row["city"]

        age = int(
            row["age"]
        )

        # ----------------------------------
        # Find current version
        # ----------------------------------

        cursor.execute(
            """
            SELECT
                customer_key,
                customer_name,
                city,
                age
            FROM dim_customer
            WHERE customer_id = %s
              AND is_current = TRUE
            """,
            (customer_id,)
        )

        existing = cursor.fetchone()

        # ==================================
        # NEW CUSTOMER
        # ==================================

        if existing is None:

            cursor.execute(
                """
                SELECT
                    COALESCE(
                        MAX(customer_key),
                        0
                    ) + 1
                FROM dim_customer
                """
            )

            new_customer_key = (
                cursor.fetchone()[0]
            )

            cursor.execute(
                """
                INSERT INTO dim_customer
                (
                    customer_key,
                    customer_id,
                    customer_name,
                    city,
                    age,
                    start_date,
                    end_date,
                    is_current
                )
                VALUES
                (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    NULL,
                    TRUE
                )
                """,
                (
                    new_customer_key,
                    customer_id,
                    customer_name,
                    city,
                    age,
                    effective_date
                )
            )

            print(
                f"New customer inserted: {customer_id}"
            )

        # ==================================
        # EXISTING CUSTOMER
        # ==================================

        else:

            current_key = existing[0]

            current_name = existing[1]

            current_city = existing[2]

            current_age = existing[3]

            # ----------------------------------
            # Check for changes
            # ----------------------------------

            changed = (
                current_name != customer_name
                or current_city != city
                or current_age != age
            )

            # ==================================
            # NO CHANGE
            # ==================================

            if not changed:

                print(
                    f"No change: {customer_id}"
                )

            # ==================================
            # CUSTOMER CHANGED
            # ==================================

            else:

                # Close old version
                cursor.execute(
                    """
                    UPDATE dim_customer
                    SET
                        end_date = %s,
                        is_current = FALSE
                    WHERE customer_key = %s
                    """,
                    (
                        effective_date,
                        current_key
                    )
                )

                # Generate next surrogate key
                cursor.execute(
                    """
                    SELECT
                        COALESCE(
                            MAX(customer_key),
                            0
                        ) + 1
                    FROM dim_customer
                    """
                )

                new_customer_key = (
                    cursor.fetchone()[0]
                )

                # Insert new version
                cursor.execute(
                    """
                    INSERT INTO dim_customer
                    (
                        customer_key,
                        customer_id,
                        customer_name,
                        city,
                        age,
                        start_date,
                        end_date,
                        is_current
                    )
                    VALUES
                    (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        NULL,
                        TRUE
                    )
                    """,
                    (
                        new_customer_key,
                        customer_id,
                        customer_name,
                        city,
                        age,
                        effective_date
                    )
                )

                print(
                    f"Customer changed: "
                    f"{customer_id} "
                    f"-> new version created"
                )

    connection.commit()

    cursor.close()
    connection.close()


# =========================================================
# GET CURRENT CUSTOMER KEYS
# =========================================================

def get_current_customer_keys():

    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT
            customer_id,
            customer_key
        FROM dim_customer
        WHERE is_current = TRUE
        """
    )

    rows = cursor.fetchall()

    cursor.close()
    connection.close()

    return rows


# =========================================================
# LOAD PRODUCT DIMENSION
# =========================================================

def load_products(df):

    connection = get_connection()
    cursor = connection.cursor()

    for _, row in df.iterrows():

        cursor.execute(
            """
            INSERT INTO dim_product
            (
                product_key,
                product_id,
                product_name,
                category,
                price
            )
            VALUES
            (
                %s,
                %s,
                %s,
                %s,
                %s
            )
            ON CONFLICT (product_id)
            DO UPDATE SET
                product_name =
                    EXCLUDED.product_name,
                category =
                    EXCLUDED.category,
                price =
                    EXCLUDED.price
            """,
            (
                int(row["product_key"]),
                int(row["product_id"]),
                row["product_name"],
                row["category"],
                float(row["price"])
            )
        )

    connection.commit()

    cursor.close()
    connection.close()

    print(
        f"{len(df)} products loaded!"
    )


# =========================================================
# LOAD DATE DIMENSION
# =========================================================

def load_dates(df):

    connection = get_connection()
    cursor = connection.cursor()

    for _, row in df.iterrows():

        cursor.execute(
            """
            INSERT INTO dim_date
            (
                date_key,
                full_date,
                day,
                month,
                quarter,
                year
            )
            VALUES
            (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            )
            ON CONFLICT (date_key)
            DO NOTHING
            """,
            (
                int(row["date_key"]),
                row["full_date"],
                int(row["day"]),
                int(row["month"]),
                int(row["quarter"]),
                int(row["year"])
            )
        )

    connection.commit()

    cursor.close()
    connection.close()

    print(
        f"{len(df)} dates loaded!"
    )


# =========================================================
# LOAD FACT SALES
# =========================================================

def load_fact_sales(df):

    connection = get_connection()
    cursor = connection.cursor()

    for _, row in df.iterrows():

        cursor.execute(
            """
            INSERT INTO fact_sales
            (
                order_id,
                customer_key,
                product_key,
                date_key,
                quantity,
                sales_amount
            )
            VALUES
            (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            )
            ON CONFLICT (order_id)
            DO NOTHING
            """,
            (
                int(row["order_id"]),
                int(row["customer_key"]),
                int(row["product_key"]),
                int(row["date_key"]),
                int(row["quantity"]),
                float(row["amount"])
            )
        )

    connection.commit()

    cursor.close()
    connection.close()

    print(
        f"{len(df)} sales records processed!"
    )