import pandas as pd

from transform import (
    transform_orders,
    transform_customers,
    transform_products,
)


def test_transform_orders():

    df = pd.DataFrame({
        "order_id": [1, 2],
        "customer_id": [101, 102],
        "product_id": [1, 2],
        "order_date": [
            "2026-09-01",
            "2026-09-02"
        ],
        "quantity": ["2", "3"],
        "amount": ["100.50", "200.00"]
    })

    result = transform_orders(df)

    assert len(result) == 2

    assert pd.api.types.is_datetime64_any_dtype(
        result["order_date"]
    )

    assert result["quantity"].dtype.kind in "iu"

    assert result["amount"].dtype.kind in "fc"


def test_transform_orders_removes_duplicates():

    df = pd.DataFrame({
        "order_id": [1, 1],
        "customer_id": [101, 101],
        "product_id": [1, 1],
        "order_date": [
            "2026-09-01",
            "2026-09-01"
        ],
        "quantity": [2, 2],
        "amount": [100, 100]
    })

    result = transform_orders(df)

    assert len(result) == 1


def test_transform_customers():

    df = pd.DataFrame({
        "customer_id": [101],
        "name": ["Test Customer"],
        "city": ["Mumbai"],
        "age": [25]
    })

    result = transform_customers(df)

    assert len(result) == 1

    assert "customer_id" in result.columns


def test_transform_products():

    df = pd.DataFrame({
        "product_id": [1],
        "product_name": ["Laptop"],
        "category": ["Electronics"],
        "price": [50000]
    })

    result = transform_products(df)

    assert len(result) == 1

    assert "product_id" in result.columns