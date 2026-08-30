import pandas as pd


def transform_customers(df):

    df = df.copy()

    # Clean customer name
    df["name"] = (
        df["name"]
        .str.strip()
    )

    # Standardize city
    df["city"] = (
        df["city"]
        .str.strip()
        .str.title()
    )

    # Remove duplicate customers
    df = df.drop_duplicates(
        subset=["customer_id"]
    )

    # Validate age
    df = df[
        (df["age"] >= 0) &
        (df["age"] <= 120)
    ]

    return df


def transform_products(df):

    df = df.copy()

    # Clean product name
    df["product_name"] = (
        df["product_name"]
        .str.strip()
    )

    # Standardize category
    df["category"] = (
        df["category"]
        .str.strip()
        .str.title()
    )

    # Remove duplicate products
    df = df.drop_duplicates(
        subset=["product_id"]
    )

    # Validate price
    df = df[
        df["price"] >= 0
    ]

    return df


def transform_orders(df):

    df = df.copy()

    # Convert order date
    df["order_date"] = pd.to_datetime(
        df["order_date"]
    )

    # Convert numeric fields
    df["quantity"] = (
        df["quantity"]
        .astype(int)
    )

    df["amount"] = (
        df["amount"]
        .astype(float)
    )

    # Remove duplicate orders
    df = df.drop_duplicates(
        subset=["order_id"]
    )

    # Validate quantity
    df = df[
        df["quantity"] > 0
    ]

    # Validate amount
    df = df[
        df["amount"] >= 0
    ]

    return df


def add_product_keys(df):

    df = df.copy()

    df["product_key"] = range(
        1,
        len(df) + 1
    )

    return df


def create_date_dimension(orders_df):

    dates = pd.DataFrame()

    dates["full_date"] = (
        orders_df["order_date"]
        .drop_duplicates()
        .sort_values()
        .reset_index(drop=True)
    )

    dates["date_key"] = (
        dates["full_date"]
        .dt.strftime("%Y%m%d")
        .astype(int)
    )

    dates["day"] = (
        dates["full_date"]
        .dt.day
    )

    dates["month"] = (
        dates["full_date"]
        .dt.month
    )

    dates["quarter"] = (
        dates["full_date"]
        .dt.quarter
    )

    dates["year"] = (
        dates["full_date"]
        .dt.year
    )

    return dates[
        [
            "date_key",
            "full_date",
            "day",
            "month",
            "quarter",
            "year"
        ]
    ]


def create_fact_sales(
    orders_df,
    customer_keys_df,
    products_df
):

    fact = orders_df.copy()

    # Add CURRENT customer surrogate key
    fact = fact.merge(
        customer_keys_df[
            [
                "customer_id",
                "customer_key"
            ]
        ],
        on="customer_id",
        how="left"
    )

    # Add product surrogate key
    fact = fact.merge(
        products_df[
            [
                "product_id",
                "product_key"
            ]
        ],
        on="product_id",
        how="left"
    )

    # Create date key
    fact["date_key"] = (
        fact["order_date"]
        .dt.strftime("%Y%m%d")
        .astype(int)
    )

    # Make sure every order found a customer
    if fact["customer_key"].isnull().any():

        missing = fact[
            fact["customer_key"].isnull()
        ]["customer_id"].tolist()

        raise ValueError(
            f"Missing customer keys for: {missing}"
        )

    # Make sure every order found a product
    if fact["product_key"].isnull().any():

        missing = fact[
            fact["product_key"].isnull()
        ]["product_id"].tolist()

        raise ValueError(
            f"Missing product keys for: {missing}"
        )

    return fact[
        [
            "order_id",
            "customer_key",
            "product_key",
            "date_key",
            "quantity",
            "amount"
        ]
    ]