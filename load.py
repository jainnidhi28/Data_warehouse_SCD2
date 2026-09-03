import psycopg

from config import (
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PASSWORD
)


# ============================================================
# DATABASE CONNECTION
# ============================================================

def get_connection():

    return psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


# ============================================================
# CREATE WAREHOUSE TABLES
# ============================================================

def create_warehouse_tables():

    connection = get_connection()

    try:

        with connection.cursor() as cursor:

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dim_customer (
                    customer_key SERIAL PRIMARY KEY,
                    customer_id INT NOT NULL,
                    customer_name VARCHAR(100),
                    city VARCHAR(100),
                    effective_date DATE,
                    end_date DATE,
                    is_current BOOLEAN
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dim_product (
                    product_key SERIAL PRIMARY KEY,
                    product_id INT UNIQUE NOT NULL,
                    product_name VARCHAR(100),
                    category VARCHAR(100),
                    price NUMERIC(10,2)
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dim_date (
                    date_key INT PRIMARY KEY,
                    full_date DATE UNIQUE NOT NULL,
                    year INT,
                    month INT,
                    day INT
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS fact_sales (
                    sales_key SERIAL PRIMARY KEY,
                    order_id INT UNIQUE NOT NULL,
                    customer_key INT,
                    product_key INT,
                    date_key INT,
                    quantity INT,
                    sales_amount NUMERIC(12,2),

                    FOREIGN KEY (customer_key)
                        REFERENCES dim_customer(customer_key),

                    FOREIGN KEY (product_key)
                        REFERENCES dim_product(product_key),

                    FOREIGN KEY (date_key)
                        REFERENCES dim_date(date_key)
                )
            """)

        connection.commit()

        print("Warehouse tables created successfully!")

    except Exception:

        connection.rollback()

        print(
            "Failed to create warehouse tables. "
            "Transaction rolled back."
        )

        raise

    finally:

        connection.close()


# ============================================================
# CUSTOMER SCD TYPE 2
# ============================================================

def process_customer_scd2(df):

    from datetime import date

    connection = get_connection()

    try:

        with connection.cursor() as cursor:

            effective_date = date.today()

            for _, row in df.iterrows():

                customer_id = int(
                    row["customer_id"]
                )

                # IMPORTANT:
                # Source/transformed column is "name"
                customer_name = row["name"]

                city = row["city"]

                age = int(
                    row["age"]
                )

                # --------------------------------------------
                # Find current customer version
                # --------------------------------------------

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

                # --------------------------------------------
                # New customer
                # --------------------------------------------

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
                        f"New customer inserted: "
                        f"{customer_id}"
                    )

                # --------------------------------------------
                # Existing customer
                # --------------------------------------------

                else:

                    current_key = existing[0]
                    current_name = existing[1]
                    current_city = existing[2]
                    current_age = existing[3]

                    # ----------------------------------------
                    # Check for changes
                    # ----------------------------------------

                    changed = (
                        current_name != customer_name
                        or current_city != city
                        or current_age != age
                    )

                    # ----------------------------------------
                    # No change
                    # ----------------------------------------

                    if not changed:

                        print(
                            f"No change: "
                            f"{customer_id}"
                        )

                    # ----------------------------------------
                    # Customer changed
                    # ----------------------------------------

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
                            f"-> new SCD2 version created"
                        )

        # ----------------------------------------------------
        # Commit entire transaction
        # ----------------------------------------------------

        connection.commit()

        print(
            "Customer SCD2 processing "
            "completed successfully!"
        )

    except Exception:

        # ----------------------------------------------------
        # Roll back entire transaction
        # ----------------------------------------------------

        connection.rollback()

        print(
            "Customer SCD2 processing failed. "
            "Transaction rolled back."
        )

        raise

    finally:

        connection.close()


# ============================================================
# GET CURRENT CUSTOMER KEYS
# ============================================================

def get_current_customer_keys():

    connection = get_connection()

    try:

        with connection.cursor() as cursor:

            cursor.execute("""
                SELECT
                    customer_id,
                    customer_key
                FROM dim_customer
                WHERE is_current = TRUE
            """)

            rows = cursor.fetchall()

        return rows

    finally:

        connection.close()

# =========================================================
# LOAD PRODUCT DIMENSION
# =========================================================

def load_products(df):

    connection = get_connection()

    try:

        with connection.cursor() as cursor:

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

        print(
            f"{len(df)} products loaded successfully!"
        )

    except Exception:

        connection.rollback()

        print(
            "Product load failed. "
            "Transaction rolled back."
        )

        raise

    finally:

        connection.close()
# ============================================================
# LOAD DATE DIMENSION
# ============================================================

def load_dates(df):

    connection = get_connection()

    try:

        with connection.cursor() as cursor:

            for _, row in df.iterrows():

                cursor.execute("""
                    INSERT INTO dim_date
                    (
                        date_key,
                        full_date,
                        year,
                        month,
                        day
                    )
                    VALUES (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s
                    )

                    ON CONFLICT (date_key)
                    DO NOTHING
                """, (
                    int(row["date_key"]),
                    row["full_date"],
                    int(row["year"]),
                    int(row["month"]),
                    int(row["day"])
                ))

        connection.commit()

        print(
            f"{len(df)} dates loaded successfully!"
        )

    except Exception:

        connection.rollback()

        print(
            "Date load failed. "
            "Transaction rolled back."
        )

        raise

    finally:

        connection.close()


# ============================================================
# LOAD FACT SALES
# TRANSACTION SAFE + UPSERT
# ============================================================

def load_fact_sales(df):

    connection = get_connection()

    try:

        with connection.cursor() as cursor:

            for _, row in df.iterrows():

                cursor.execute("""
                    INSERT INTO fact_sales
                    (
                        order_id,
                        customer_key,
                        product_key,
                        date_key,
                        quantity,
                        sales_amount
                    )
                    VALUES (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s
                    )

                    ON CONFLICT (order_id)
                    DO UPDATE SET
                        customer_key = EXCLUDED.customer_key,
                        product_key = EXCLUDED.product_key,
                        date_key = EXCLUDED.date_key,
                        quantity = EXCLUDED.quantity,
                        sales_amount = EXCLUDED.sales_amount
                """, (
                    int(row["order_id"]),
                    int(row["customer_key"]),
                    int(row["product_key"]),
                    int(row["date_key"]),
                    int(row["quantity"]),
                    float(row["amount"])
                ))

        # ----------------------------------------------------
        # Commit only after ALL rows succeed
        # ----------------------------------------------------

        connection.commit()

        print(
            f"{len(df)} sales records "
            f"upserted successfully!"
        )

    except Exception:

        # ----------------------------------------------------
        # Roll back the entire transaction
        # ----------------------------------------------------

        connection.rollback()

        print(
            "Fact load failed. "
            "Transaction rolled back."
        )

        raise

    finally:

        connection.close()


# ============================================================
# PIPELINE METADATA TABLE
# ============================================================

def create_pipeline_metadata_table():

    connection = get_connection()

    try:

        with connection.cursor() as cursor:

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_metadata (
                    pipeline_name VARCHAR(100) PRIMARY KEY,
                    last_processed_date DATE,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

        connection.commit()

        print(
            "Pipeline metadata table "
            "created successfully!"
        )

    except Exception:

        connection.rollback()

        print(
            "Failed to create pipeline metadata table."
        )

        raise

    finally:

        connection.close()


# ============================================================
# GET PIPELINE WATERMARK
# ============================================================

def get_pipeline_watermark(pipeline_name):

    connection = get_connection()

    try:

        with connection.cursor() as cursor:

            cursor.execute("""
                SELECT last_processed_date
                FROM pipeline_metadata
                WHERE pipeline_name = %s
            """, (pipeline_name,))

            result = cursor.fetchone()

            if result is None or result[0] is None:

                return "1900-01-01"

            return result[0].strftime("%Y-%m-%d")

    finally:

        connection.close()


# ============================================================
# UPDATE PIPELINE WATERMARK
# ============================================================

def set_pipeline_watermark(
    pipeline_name,
    watermark
):

    connection = get_connection()

    try:

        with connection.cursor() as cursor:

            cursor.execute("""
                INSERT INTO pipeline_metadata
                (
                    pipeline_name,
                    last_processed_date,
                    updated_at
                )
                VALUES (
                    %s,
                    %s,
                    CURRENT_TIMESTAMP
                )

                ON CONFLICT (pipeline_name)
                DO UPDATE SET
                    last_processed_date =
                        EXCLUDED.last_processed_date,
                    updated_at =
                        CURRENT_TIMESTAMP
            """, (
                pipeline_name,
                watermark
            ))

        connection.commit()

        print(
            f"Pipeline watermark updated: "
            f"{pipeline_name} -> {watermark}"
        )

    except Exception:

        connection.rollback()

        print(
            "Failed to update pipeline watermark."
        )

        raise

    finally:

        connection.close()