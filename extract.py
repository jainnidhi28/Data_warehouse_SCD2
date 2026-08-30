import pandas as pd


def extract_customers():

    return pd.read_csv(
        "data/customers.csv"
    )


def extract_products():

    return pd.read_csv(
        "data/products.csv"
    )


def extract_orders():

    return pd.read_csv(
        "data/orders.csv"
    )