import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"

def extract_customers():

    return pd.read_csv(DATA_DIR / "customers.csv")


def extract_products():

    return pd.read_csv(DATA_DIR / "products.csv")


def extract_orders():

    return pd.read_csv(DATA_DIR / "orders.csv")